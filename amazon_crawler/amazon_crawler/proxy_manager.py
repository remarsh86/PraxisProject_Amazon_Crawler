from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
#from fake_useragent import UserAgent

import time
from threading import Thread
import random



class ProxyPool:
    class __ProxyPool:
        def __init__(self):
            self.proxies = []
            self.ua = UserAgent()
            self.current_proxy = None

        def __str__(self):
            return repr(self) + self.val

        def get_random_proxy(self, update=False):
            print("someone called me..................")
            self.get_sslproxies()

            if update == True and self.current_proxy is not None:
                self.proxies.remove(self.current_proxy)
                self.current_proxy = None
                return self.get_random_proxy()

            if self.current_proxy is None:
                self.current_proxy = random.choice(self.proxies)

            return self.current_proxy

        def get_sslproxies(self):
            if len(self.proxies) == 0:
                print("fetching sslproxies.org proxies")
                proxies_req = Request('https://www.sslproxies.org/')
                proxies_req.add_header('User-Agent', self.ua.random)
                proxies_doc = urlopen(proxies_req).read().decode('utf8')
                proxies_table = BeautifulSoup(proxies_doc, 'html.parser').find(id='proxylisttable')
                for row in proxies_table.tbody.find_all('tr'):
                    https = ip = row.find_all('td')[6].string
                    if https == 'yes':
                        ip = row.find_all('td')[0].string
                        port = row.find_all('td')[1].string
                        self.proxies.append("https://{}:{}".format(ip, port))
                print("collected proxies: ", len(self.proxies))
            return self.proxies


    instance = None

    def __init__(self):
        if not ProxyPool.instance:
            ProxyPool.instance = ProxyPool.__ProxyPool()

    def __getattr__(self, name):
        return getattr(self.instance, name)


if __name__ == '__main__':
    p = ProxyPool()
    print(p.get_random_proxy())
