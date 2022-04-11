from argparse import ArgumentParser

from queue import Queue
from threading import Thread, Lock

from abc import ABC
from time import sleep
from hashlib import md5
from typing import Optional, Tuple
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from html.parser import HTMLParser

worker_free_max_seconds = 30
parsed_links = set()
links_queue = Queue()
lock = Lock()


class LinksHTMLParser(HTMLParser, ABC):
    def __init__(self, domain) -> None:
        super().__init__()
        self.domain = domain
        self.links = []

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag != 'a':
            return

        link = LinkService.search_for_endpoint(attrs)
        if not link:
            return

        if LinkService.compare_domains(link, self.domain):
            self.links.append(link)

    def search_in_html(self, html: str) -> None:
        self.links = []
        self.feed(html)


class LinkService:
    @staticmethod
    def process_domain(domain: str) -> str:
        if len(domain) > 4 and domain[:4] == 'www.':
            return domain[4:]
        return domain

    @staticmethod
    def clear_link(link: str) -> str:
        if link[:2] == '//':
            return f"http:{link}"
        return link

    @staticmethod
    def hash_link(link: str) -> bytes:
        return md5(link.encode('utf-8')).digest()

    @staticmethod
    def compare_domains(first: str, second: str) -> bool:
        parsed_url = urlparse(first)
        first = LinkService.process_domain(parsed_url.netloc)

        first_split = first.split('.')
        second_split = second.split('.')

        return '.'.join(first_split[-2:]) == '.'.join(second_split[-2:])

    @staticmethod
    def search_for_endpoint(attrs: list) -> str:
        for name, value in attrs:
            if name == 'href':
                return value


class QueueService:
    @staticmethod
    def fill_queue(links_parser: LinksHTMLParser) -> None:
        global links_queue

        if not links_parser.links:
            return

        with lock:
            for link in links_parser.links:
                if LinkService.hash_link(link) not in parsed_links:
                    links_queue.put(link)
            links_parser.links.clear()

    @staticmethod
    def get_next_link() -> Optional[str]:
        global parsed_links

        with lock:
            if links_queue.empty():
                return
            link = links_queue.get(block=False)
            hash_link = LinkService.hash_link(link)
            if hash_link in parsed_links:
                return
            parsed_links.add(hash_link)
            print(link)
            return link


def get_url_data(url: str) -> Tuple[Optional[str], Optional[str]]:
    request = Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
        (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36'
    })
    try:
        with urlopen(request) as response:
            raw = response.read()
            return str(raw), response.url
    except Exception:
        return None, None


def handler(domain) -> None:
    seconds_free = 0
    links_parser = LinksHTMLParser(domain)

    while seconds_free < worker_free_max_seconds or not links_queue.empty():
        link = QueueService.get_next_link()

        if not link:
            sleep(1)
            seconds_free += 1
            continue
        seconds_free = 0

        link = LinkService.clear_link(link)

        html, response_link = get_url_data(link)
        if not html:
            continue

        if response_link and response_link != link:
            current_domain = LinkService.process_domain(response_link)
            if not LinkService.compare_domains(current_domain, links_parser.domain):
                continue

        links_parser.search_in_html(html)

        QueueService.fill_queue(links_parser)


def start(url: str, total_workers: int) -> None:
    links_queue.put(url)

    domain = urlparse(url).netloc
    domain = LinkService.process_domain(domain)

    threads = []
    for index in range(total_workers):
        thread = Thread(target=handler, args=(domain,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-n', type=int, default=1)
    parser.add_argument('url')
    args = parser.parse_args()
    start(args.url, args.n)
