# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from ftp_scraper.db.base import loadsession, Base
from ftp_scraper.db.ftpUo import FtpUo

class FtpScraperPipeline:
    def __init__(self) -> None:
        self.session = loadsession()
        self.in_db = dict(self.session.query(FtpUo.folder_link, FtpUo.id).all())

    def process_item(self, item, spider):
        session = self.session
        if item['folder_link'] not in self.in_db:
            folder = FtpUo(item['folder_name'], item['folder_link'])
            session.add(folder)       
            session.commit()
        return item   
        
