# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals  # Import Scrapy signals to hook into events (like spider opening).

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter  # Import helper to work with both ScrapyItems and Dicts uniformly.


class ImdbScraperSpiderMiddleware:  # Middleware that sits between the Engine and the Spider.
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):  # Factory method used by Scrapy to create this middleware instance.
        # This method is used by Scrapy to create your spiders.
        s = cls()  # Instantiate the class.
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)  # Connect the 'spider_opened' signal to our handler.
        return s  # Return the middleware instance.

    def process_spider_input(self, response, spider):  # Called for each response going TO the spider.
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None  # We don't need to modify responses here, so return None.

    def process_spider_output(self, response, result, spider):  # Called with the result (items/requests) COMING FROM the spider.
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:  # Iterate over everything the spider yielded (items or new requests).
            yield i  # Pass them along unchanged.

    def process_spider_exception(self, response, exception, spider):  # Called if the spider raises an exception.
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass  # We don't handle exceptions here, so just pass.

    async def process_start(self, start):  # Called with the start requests of the spider.
        # Called with an async iterator over the spider start() method or the
        # matching method of an earlier spider middleware.
        async for item_or_request in start:  # Iterate over the start requests.
            yield item_or_request  # Yield them unchanged.

    def spider_opened(self, spider):  # Handler called when the spider opens.
        spider.logger.info("Spider opened: %s" % spider.name)  # Log that the spider has started.


class ImdbScraperDownloaderMiddleware:  # Middleware that sits between the Engine and the Downloader (network).
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):  # Factory method to create the middleware and connect signals.
        # This method is used by Scrapy to create your spiders.
        s = cls()  # Create instance.
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)  # Bind signal.
        return s  # Return instance.

    def process_request(self, request, spider):  # Called for every request going TO the internet.
        # Use Bright Data proxy for every request (also for Metacritic)
        request.meta["proxy"] = spider.settings.get("PROXY_URL")  # Set the HTTP proxy from settings.
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None  # Return None to let Scrapy process the request normally (but with our proxy set).

    def process_response(self, request, response, spider):  # Called with the response COMING FROM the internet.
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response  # Return the response unchanged.

    def process_exception(self, request, exception, spider):  # Called if the download fails (e.g., DNS error, timeout).
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass  # We let Scrapy's default retry middleware handle exceptions (like timeouts).

    def spider_opened(self, spider):  # Handler for spider open signal.
        spider.logger.info("Spider opened: %s" % spider.name)  # Log info.
