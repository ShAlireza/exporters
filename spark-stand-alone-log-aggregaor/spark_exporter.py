import yaml
import requests

from prometheus_client import Gauge, CollectorRegistry, generate_latest

from scrape import run_master_spider


def add_word(word, string, index):
    return string[:index] + word + string[index:]


def add_word_to_list_of_strings(word, list_of_strings, index):
    new_list = []
    for item in list_of_strings:
        new_list.append(add_word(word, item, index))

    return new_list


def merge_metrics(list_of_metrics):
    return '\n'.join(list_of_metrics) + '\n'


class SparkExporter:
    def __init__(
            self,
            *args,
            **kwargs,
    ):
        self.host: str = kwargs.get('host')
        self.name: str = kwargs.get('name')
        self.raw_data: dict = kwargs.get('raw_data')

    def export_metrics(self, *args, **kwargs):
        raise NotImplementedError

    def process_metrics(self, response_text):
        metrics_list = list(filter(lambda x: x, response_text.split("\n")))

        return merge_metrics(add_word_to_list_of_strings(
            "_" + self.name, metrics_list, index=7
        ))


class SparkExporterMaster(SparkExporter):
    def __init__(
            self,
            host: str,
            name: str,
            raw_data: dict,
            metrics_path: str = '/metrics/master/prometheus',
            applications_metrics_path: str = '/metrics/applications/prometheus'
    ):
        super().__init__(host=host, name=name, raw_data=raw_data)

        self.metrics_path = metrics_path
        self.applications_metrics_path = applications_metrics_path

    def export_metrics(self, *args, **kwargs):
        response_text = ""

        response = requests.get(
            f'{self.host}{self.metrics_path}'
        )
        response_text += response.text
        response = requests.get(
            f'{self.host}{self.applications_metrics_path}'
        )
        response_text += response.text

        return self.process_metrics(response_text)


class SparkExporterWorker(SparkExporter):
    def __init__(
            self,
            host: str,
            name: str,
            raw_data: dict,
            metrics_path: str = '/metrics/prometheus'
    ):
        super().__init__(host=host, name=name, raw_data=raw_data)

        self.metrics_path = metrics_path

    def export_metrics(self, *args, **kwargs):
        response = requests.get(
            f'{self.host}{self.metrics_path}'
        )

        response_text = response.text

        return self.process_metrics(response_text)


class SparkExporterHistory(SparkExporter):
    def __init__(
            self,
            host: str,
            name: str,
            raw_data: dict,
            api_path: str = '/api/v1'
    ):
        super().__init__(host=host, name=name, raw_data=raw_data)
        self.api_path = api_path

    def export_metrics(self, *args, **kwargs):
        return ""

    def get_running_applications(self, **kwargs):
        filter_applications = kwargs.get('filter')

        response = requests.get(
            f'{self.host}{self.api_path}/applications',
            params={
                'status': 'running'
            }
        )

        if response.status_code != 200:
            print(response.status_code, response.text)
            return

        apps = response.json()

        if filter_applications:
            apps = list(filter(filter_applications, apps))

        return apps

    def get_application_executors(self, app_id):
        response = requests.get(
            f'{self.host}{self.api_path}/applications/{app_id}/executors'
        )

        return response.json()

    def get_application_environment(self, app_id):
        response = requests.get(
            f'{self.host}{self.api_path}/applications/{app_id}'
            f'/environment'
        )

        return response.json()


class SparkExporterApplication(SparkExporter):
    def __init__(
            self,
            host: str,
            name: str,
            raw_data: dict,
            metrics_path: str,
            exported_data: str = None
    ):
        super().__init__(host=host, name=name, raw_data=raw_data)
        self.metrics_path = metrics_path
        self.exported_data = exported_data

    def export_metrics(self, *args, **kwargs):
        if self.exported_data:
            return self.exported_data

        response = requests.get(
            f'{self.host}{self.metrics_path}'
        )

        response_text = response.text

        return response_text


class SparkExporterCluster(SparkExporter):
    def __init__(
            self,
            raw_data: dict,
            name: str,
            max_ports_search: int = 10,
            application_base_port: int = 4040,
            workers=None,
            masters=None,
            histories=None,
            applications=None,
            static_applications=None,
    ):
        super().__init__(name=name, raw_data=raw_data, host="")
        self.max_ports_search = max_ports_search
        self.application_base_port = application_base_port
        self.workers = workers
        self.masters = masters
        self.histories = histories
        self.applications = applications
        self.static_applications = static_applications
        self.active_master = None
        self.running_applications = []
        self.finished_applications = []

    def find_active_master(self):
        with requests.Session() as session:

            for master in self.masters:
                response = session.get(f'{master.host}')
                if 'ALIVE' in response.text:
                    self.active_master = master.host
                    break

    def export_metrics(self, *args, **kwargs):
        response_text = ""
        for spark in self.sparks:
            response_text += spark.export_metrics()

        response_text += self.static_apps_up()

        return self.process_metrics(response_text)

    def static_apps_up(self):
        registry = CollectorRegistry()
        gauge = Gauge(f'metrics_app_up', 'Application up or down',
                      ['cluster', 'app_name'], registry=registry)

        for static_app in self.static_applications:
            if self.app_is_running(static_app):
                gauge.labels(self.name, static_app).set(1)
            else:
                gauge.labels(self.name, static_app).set(0)

        return generate_latest(registry=registry).decode('utf-8')

    def app_is_running(self, app_name):
        for app in self.running_applications:
            if app.name == app_name:
                return True

        return False

    @property
    def sparks(self):
        self.update_running_applications()

        return (self.workers + self.masters + self.histories +
                self.applications + self.running_applications)

    def update_running_applications(self):
        self.running_applications = []
        self.find_active_master()
        applications, applications_names, *_ = run_master_spider(
            start_urls=[self.active_master]
        )
        for host, name in zip(applications, applications_names):
            self.add_app(
                host=host,
                name=name
            )

    def add_app(
            self,
            host: str,
            name: str,
            raw_data: dict = None,
            metrics_path: str = '/metrics/prometheus',
            exported_data: str = None
    ):
        application = SparkExporterApplication(
            host=host,
            name=name,
            raw_data=raw_data,
            metrics_path=metrics_path,
            exported_data=exported_data
        )
        self.running_applications.append(application)

        return application


def load_config(file_path='config.yaml'):
    with open(file_path, 'r') as stream:
        data = yaml.safe_load(stream)

    cluster_configs = []

    for cluster_raw_data in data['clusters']:
        print(cluster_raw_data)
        cluster_conf = SparkExporterCluster(
            raw_data=cluster_raw_data,
            name=cluster_raw_data.get('name'),
            max_ports_search=cluster_raw_data.get('max_ports_search'),
            application_base_port=cluster_raw_data.get('application_base_port'),
            workers=[],
            masters=[],
            histories=[],
            applications=[],
            static_applications=[]
        )
        for master_raw_data in cluster_raw_data.get('masters'):
            master_conf = SparkExporterMaster(
                raw_data=master_raw_data,
                host=master_raw_data.get('host'),
                name=master_raw_data.get('name'),
                metrics_path=master_raw_data.get('metrics_path'),
                applications_metrics_path=master_raw_data.get(
                    'applications_metrics_path'
                )
            )

            cluster_conf.masters.append(master_conf)

        for worker_raw_data in cluster_raw_data.get('workers'):
            worker_conf = SparkExporterWorker(
                raw_data=worker_raw_data,
                host=worker_raw_data.get('host'),
                name=worker_raw_data.get('name'),
                metrics_path=worker_raw_data.get('metrics_path')
            )
            cluster_conf.workers.append(worker_conf)

        for history_raw_data in cluster_raw_data.get('histories'):
            history_conf = SparkExporterHistory(
                raw_data=history_raw_data,
                host=history_raw_data.get('host'),
                name=history_raw_data.get('name'),
                api_path=history_raw_data.get('api_path')
            )
            cluster_conf.histories.append(history_conf)

        for application_raw_data in cluster_raw_data.get('applications'):
            application_conf = SparkExporterApplication(
                raw_data=application_raw_data,
                host=application_raw_data.get('host'),
                name=application_raw_data.get('name'),
                metrics_path=application_raw_data.get('metrics_path')
            )

            cluster_conf.applications.append(application_conf)
        for static_application in cluster_raw_data.get('static_applications'):
            cluster_conf.static_applications.append(static_application)

        cluster_configs.append(cluster_conf)

    return cluster_configs


clusters = load_config()

print(clusters)
