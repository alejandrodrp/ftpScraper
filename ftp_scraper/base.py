import re
from numpy import nan
from pandas import DataFrame
from sqlalchemy import create_engine, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.engine import Engine

#import private as pv
#from util.times import get_time

engine = create_engine(
    f'postgresql://postgres:1234567890@localhost:5432/FtpUoDatabase')
# engine = create_engine(f'postgresql://postgres:{pv.DB_PRO_PW}@localhost:5434/DATA')
# engine = create_engine(f'postgresql://postgres:{pv.DB_DES_PW}@lpvm.duckdns.org:5433/DATA')


def initializer(engine: Engine):
    """ensure the parent proc's database connections are not touched
    in the new connection pool"""
    engine.dispose(close=False)


def loadsession() -> Session:
    """
    Conecta con la base de datos.

    Returns:
        Session: 
    """
    session_factory = sessionmaker(bind=engine, autoflush=False)
    return session_factory()


def scopedSession() -> tuple:
    engine = create_engine(
        f'postgresql://postgres:{pv.DB_DES_PW}@localhost:5433/DATA')
    # engine = create_engine(f'postgresql://postgres:{pv.DB_PRO_PW}@localhost:5434/DATA')
    # engine = create_engine(f'postgresql://postgres:{pv.DB_DES_PW}@lpvm.duckdns.org:5433/DATA')
    session_factory = sessionmaker(bind=engine, autoflush=False)
    Session_scope = scoped_session(session_factory)
    return Session_scope, engine


Base = declarative_base(engine)


def showAttrs(self):
    """
    Muestra todos los attr del objeto de la forma:
    ClassName:
        attr_1=value_1
        ...
        attr_n=value_n
    Returns:
        string:
    """
    class_name = self.__class__.__name__
    repr_str = f'\n{class_name}:\n'
    for col in self.__class__.__table__.columns:
        attr = col.key
        repr_str += f'\t{attr} = {getattr(self, attr)}\n'
    return repr_str


def readDF(cls, df: DataFrame, session: Session, **kw) -> None:
    """
    Lee el DF `df` y convierte cada una de sus filas en un elementop de la clase `cls`.

    Args:
        df (DataFrame): DataFrame contenedor de los datos e guardar en la DB 
        session (Session): 
    """
    BOOLEANS = kw.get('BOOLEANS', [])
    STRINGS = kw.get('STRINGS', [])
    INTEGERS = kw.get('INTEGERS', [])
    DATES = kw.get('DATES', [])

    col = (col.key for col in cls.__table__.columns if col.key != 'id')

    for row in df.values:
        kw = {}
        for k, v in zip(col, row):
            k: str
            if type(v) == type(nan):
                kw[k] = None
            elif k in BOOLEANS:
                if 'No' in v:
                    kw[k] = False
                else:
                    kw[k] = True
            elif k in STRINGS:
                kw[k] = str(v)
            elif k in INTEGERS:
                if isinstance(v, int):
                    kw[k] = v
                else:
                    kw[k] = int(re.sub(r'\D', '', v))
            elif k in DATES:
                kw[k] = v.strftime("%Y-%m-%dT%H:%M:%SZ")
        inm = cls(**kw)
        session.add(inm)


def getAttrs(self, *to_remove) -> dict:
    cls = self.__class__
    kw = {}
    for col in cls.__table__.columns:
        k = col.key
        if k in to_remove:
            continue
        kw[k] = getattr(self, k)
    return kw


class SuperVers:
    """
    Clase usada para el control de las versiones, ya sea creacion o baja.
    """

    def __init__(self, vers, fk_id, **kw) -> None:
        super().__init__(**kw)
        self.vers = vers + 1
        self.fk_id = fk_id
        self.f_creacion = get_time()

    def __str__(self) -> str:
        return showAttrs(self)

    @classmethod
    def create(cls, session: Session, fk_id: int, **kw) -> None:
        """
        Crea una version de la clase `cls`, si ya existe le asigna el numero de version
        superior inmediato.\n
        REALIZA COMMIT

        Args:
            session (Session):\n
            fk_id (int):
        """
        exist_vers = session.query(cls.vers).filter(
            cls.fk_id == fk_id
        ).order_by(
            cls.vers.desc()
        ).first()
        if exist_vers:
            exist_vers, = exist_vers
            obj = cls(exist_vers, fk_id, **kw)
        else:
            obj = cls(0, fk_id, **kw)

        session.add(obj)

    @classmethod
    def darListBaja(cls, session: Session, list_fk_id: int) -> None:
        """
        Da de baja las versiones de la clase base donde el id sea `fk_id`, 
        NO REALIZA COMMIT.

        Args:
            session (Session):\n
            fk_id (int):\n
        """
        stmt = update(cls).where(
            cls.fk_id.in_(list_fk_id)
        ).values(
            f_baja=get_time()
        )
        session.execute(stmt)
