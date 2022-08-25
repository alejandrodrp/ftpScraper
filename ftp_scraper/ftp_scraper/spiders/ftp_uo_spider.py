import scrapy

class FtpUoSpider(scrapy.Spider):
    name = 'ftp_uo'
    start_urls = ['http://ftp.uo.edu.cu/Programacion']
    allowed_domains = ['ftp.uo.edu.cu']
    custom_settings = {
        #'FEED_URI': 'data.json',
        #'FEED_FORMAT': 'json',
        'FEED_ENCODING': 'utf-8'
    }

    def parse(self, response):
        folder_names = response.xpath('//td[@class = "fb-s" and not(text())]/preceding-sibling::td[@class = "fb-n"]/a[text() != "Parent Directory"]/text()').getall()
        links = response.xpath('//td[@class = "fb-s" and not(text())]/preceding-sibling::td[@class = "fb-n"]/a[text() != "Parent Directory"]/@href').getall()
        name_link_dict = {'structured': []}
        for name in folder_names:
            current_dict = {}
            current_dict['folder_name'] = name
            current_dict['folder_link'] = response.urljoin(response.xpath(f'//td[@class = "fb-s" and not(text())]/preceding-sibling::td[@class = "fb-n"]/a[contains(@href, "{name}")]/@href').get())
            name_link_dict['structured'].append(current_dict)
            yield current_dict
        if links:
            for next_page in links:
                yield response.follow(next_page, callback = self.parse)

