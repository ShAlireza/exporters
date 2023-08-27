import requests
from parsel import Selector


class MasterSpider:
    name = 'test_scraper'
    responses = []

    def __init__(self, url, **kwargs):
        self.url = url

    def process(self):
        response = requests.get(
            url=f'{self.url}'
        )
        return self.parse(
            response=Selector(response.text)
        )

    def parse(self, response, **kwargs):
        running_apps_selector = (
            ".aggregated-activeApps > table > tbody > tr"
            " > td:nth-child(2) > a ::attr(href)"
        )
        running_apps_names_selector = (
            ".aggregated-activeApps > table > tbody > tr"
            " > td:nth-child(2) > a ::text"
        )
        completed_apps_selector = (
            ".aggregated-completedApps  > table > "
            "tbody > tr > td:nth-child(2) > a ::attr(href)"
        )
        completed_apps_names_selector = (
            ".aggregated-completedApps  > table > "
            "tbody > tr > td:nth-child(2) > a ::text"
        )

        self.responses = (
            response.css(running_apps_selector).getall(),
            response.css(running_apps_names_selector).getall(),
            response.css(completed_apps_selector).getall(),
            response.css(completed_apps_names_selector).getall()
        )
        print(self.responses)

        return self.responses


def run_master_spider(start_urls: list):
    spider = MasterSpider(
        url=start_urls[0]
    )

    return spider.process()
