from base import Base
from sqlalchemy import Column, Integer, String

class FtpUo(Base):

    __tablename__ = 'ftp_uo_data'

    id = Column(Integer, primary_key = True)
    folder_name = Column(String)
    folder_link = Column(String)

    def __init__(self, folder_name, folder_link) -> None:
        self.folder_name = folder_name
        self.folder_link = folder_link

if __name__ == '__main__':
    Base.metadata.create_all()