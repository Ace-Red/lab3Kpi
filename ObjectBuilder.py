from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict
from Database import SingletonDB
import requests
from flask_restful import reqparse
from Filter import MaximalPrice, MinimalPrice, Data, TicketsType, OriginFlight, DestinationFlight
import time


class ObjectBuilder(ABC):
    @property
    @abstractmethod
    def model(self) -> None:
        pass

    @abstractmethod
    def extract_from_source(self) -> None:
        pass

    @abstractmethod
    def reformat(self) -> None:
        pass

    @abstractmethod
    def filter(self) -> None:
        pass


class Provider1ObjectBuilder(ObjectBuilder):
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._model = OwnModel()

    @property
    def model(self) -> OwnModel:
        model = self._model
        self.reset()
        return model

    def extract_from_source(self) -> None:
        self._model.set(requests.get('http://127.0.0.1:5001/search/').json())

    def reformat(self) -> None:
        pass

    def filter(self) -> None:
        self._model.filter()


class Provider2ObjectBuilder(ObjectBuilder):
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._model = OwnModel()

    @property
    def model(self) -> OwnModel:
        model = self._model
        self.reset()
        return model

    def extract_from_source(self) -> None:
        page = [0]
        page_n = 1
        while len(page) > 0:  # and page_n <= 1:   =========================== Убрать
            page = requests.get('http://127.0.0.1:5002/price-list?page=' + str(page_n)).json()
            print(len(page))
            page_n += 1
            self._model.models += page

    def reformat(self) -> None:
        full_models = []
        print("reform")
        for row in self._model.models:
            full_models.append(requests.get('http://127.0.0.1:5002/details/' + str(row["ticket_id"])).json())  # ?
            # print(row["product_id"])
        self._model.set(full_models)

    def filter(self) -> None:
        self._model.filter()


class OwnObjectBuilder(ObjectBuilder):
    def __init__(self) -> None:
        self.reset()
        self.db = SingletonDB()

    def reset(self) -> None:
        self._model = OwnModel()

    @property
    def model(self) -> OwnModel:
        model = self._model
        self.reset()
        return model

    def extract_from_source(self) -> None:
        self._model.set(self._model.select_all_db_data())

    def reformat(self) -> None:
        my_list = []
        for row in self.model.models:
            a = {"ticket_id": row[0], "seat_number": row[1], "purchase_date": str(row[2]), "user_id": row[3],
                 "flight_id": row[4], "departure_time": str(row[5]), "origin": str(row[6]),
                 "destination": str(row[7]), "ticket_type": str(row[8]), "price": row[9],
                 "is_active": str(row[10])
                 }
            my_list.append(a)
        self._model.set(my_list)

    def filter(self) -> None:
        self._model.filter()


class Director:
    def __init__(self) -> None:
        self._builder = None

    @property
    def builder(self) -> ObjectBuilder:
        return self._builder

    @builder.setter
    def builder(self, builder: ObjectBuilder) -> None:
        self._builder = builder

    def build_all_models(self) -> None:
        self.builder.extract_from_source()
        self.builder.reformat()

    def build_filtered_model(self) -> None:
        self.builder.extract_from_source()
        self.builder.reformat()
        self.builder.filter()


class OwnModel:
    def __init__(self):
        self.models = []
        self.filtered_models = []
        self.conn = SingletonDB().conn
        self.args = {}

    def add(self, model: Dict[str, Any]):
        self.models.append(model)

    def join(self, another_model):
        self.models += another_model.models

    def drop(self, id):
        del self.models[id]

    def set(self, models):
        self.models = models

    def select_all_db_data(self):
        rows = []
        with self.conn.cursor() as cursor:
            cursor.execute(
                'SELECT "FlightLabKpi2".public."public.Ticket".ticket_id, "FlightLabKpi2".public."public.Ticket"."seat_number", "FlightLabKpi2".public."public.Ticket"."purchase_date", "FlightLabKpi2".public."public.Ticket"."user_id", "FlightLabKpi2".public."public.Ticket"."flight_id", "FlightLabKpi2".public."public.Flight"."departure_time", "FlightLabKpi2".public."public.Flight"."origin", "FlightLabKpi2".public."public.Flight"."destination","FlightLabKpi2".public."public.Ticket"."ticket_type","FlightLabKpi2".public."public.Ticket"."price","FlightLabKpi2".public."public.Ticket"."is_active" FROM "FlightLabKpi2".public."public.Ticket" INNER JOIN "FlightLabKpi2".public."public.Flight" ON "FlightLabKpi2".public."public.Flight"."flight_id" = "FlightLabKpi2".public."public.Ticket"."flight_id"')
            rows = cursor.fetchall()
        return rows

    def insert(self, args):
        with self.conn.cursor() as cursor:
            print(args)
            cursor.execute(
                '''INSERT INTO "public.Ticket"("seat_number", "purchase_date", "ticket_type", "price", "is_active", "user_id", "flight_id") VALUES(%s,'%s','%s',%s,%s,%s,%s)''' % (
                    str(args["seat_number"]), str(args["purchase_date"]), str(args["ticket_type"]),
                    str(args["price"]),
                    str(args["is_active"]),
                    str(args["user_id"]), str(args["flight_id"])))
        self.conn.commit()

        with self.conn.cursor() as cursor:
            cursor.execute(
                '''SELECT "FlightLabKpi2".public."public.Ticket".ticket_id, "FlightLabKpi2".public."public.Ticket"."seat_number", "FlightLabKpi2".public."public.Ticket"."purchase_date", "FlightLabKpi2".public."public.Ticket"."user_id", "FlightLabKpi2".public."public.Ticket"."flight_id", "FlightLabKpi2".public."public.Flight"."departure_time", "FlightLabKpi2".public."public.Flight"."origin", "FlightLabKpi2".public."public.Flight"."destination","FlightLabKpi2".public."public.Ticket"."ticket_type","FlightLabKpi2".public."public.Ticket"."price","FlightLabKpi2".public."public.Ticket"."is_active" FROM "FlightLabKpi2".public."public.Ticket" INNER JOIN "FlightLabKpi2".public."public.Flight" ON "FlightLabKpi2".public."public.Flight"."flight_id" = "FlightLabKpi2".public."public.Ticket"."flight_id"''')
            rows = cursor.fetchall()
        args = self.reform(rows[-1])

        with self.conn.cursor() as cursor:
            cursor.execute(
                '''INSERT INTO ticket_cache ("ticket_id","seat_number", "purchase_date", "ticket_type", "price", "is_active", "user_id", "flight_id","departure_time","destination","origin") VALUES (%s,%s,'%s','%s',%s,%s,%s,%s,'%s','%s','%s')''' %
                (str(int(args["ticket_id"])), str(args["seat_number"]), str(args["purchase_date"]),
                 str(args["ticket_type"]),
                 str(args["price"]),
                 str(args["is_active"]), str(args["user_id"]), str(args["flight_id"]), str(args["departure_time"]),
                 str(args["origin"]), str(args["destination"])))
        self.conn.commit()

    def delete(self, id):
        with self.conn.cursor() as cursor:
            cursor.execute('DELETE FROM "public.Ticket" WHERE "ticket_id"=' + str(id))
            cursor.execute('DELETE FROM ticket_cache WHERE "ticket_id"=' + str(id))
        self.conn.commit()

    def update(self, args):
        query_str = 'UPDATE "public.Ticket" SET '
        for key, value in args.items():
            if key != 'ticket_id' and value is not None:
                query_str += '"' + key + '"=' + "'" + str(value) + "',"
        query_str = query_str[0:-1]
        query_str += ' WHERE "ticket_id"=' + str(args["ticket_id"])
        with self.conn.cursor() as cursor:
            cursor.execute(query_str)
        self.conn.commit()

        with self.conn.cursor() as cursor:
            cursor.execute(
                '''SELECT "FlightLabKpi2".public."public.Ticket".ticket_id, "FlightLabKpi2".public."public.Ticket"."seat_number", "FlightLabKpi2".public."public.Ticket"."purchase_date", "FlightLabKpi2".public."public.Ticket"."user_id", "FlightLabKpi2".public."public.Ticket"."flight_id", "FlightLabKpi2".public."public.Flight"."departure_time", "FlightLabKpi2".public."public.Flight"."origin", "FlightLabKpi2".public."public.Flight"."destination","FlightLabKpi2".public."public.Ticket"."ticket_type","FlightLabKpi2".public."public.Ticket"."price","FlightLabKpi2".public."public.Ticket"."is_active" FROM "FlightLabKpi2".public."public.Ticket" INNER JOIN "FlightLabKpi2".public."public.Flight" ON "FlightLabKpi2".public."public.Flight"."flight_id" = "FlightLabKpi2".public."public.Ticket"."flight_id" WHERE ticket_id='%s'  ''' %
                args["ticket_id"])
            rows = cursor.fetchall()
        args = self.reform(rows[-1])

        query_str = 'UPDATE ticket_cache SET '
        for key, value in args.items():
            if key != 'ticket_id' and value != None:
                if type(value) == float:
                    value = int(value)
                query_str += '"' + key + '"=' + "'" + str(value) + "',"
        query_str = query_str[0:-1]
        query_str += ' WHERE "ticket_id"=' + str(args["ticket_id"])
        with self.conn.cursor() as cursor:
            cursor.execute(query_str)
        self.conn.commit()

    def mfilter(self, x):
        # print(len(self.filtered_products))
        product_filter = MaximalPrice() & MinimalPrice() & Data() & TicketsType() & OriginFlight() & DestinationFlight()
        if product_filter.is_satisfied_by(x, self.args):
            return x
        return None
        # self.filtered_products.append(x)

    def filter(self):
        # model_filter = MaximalPrice() & MinimalPrice() & Data() & TicketsType() & OriginFlight() & DestinationFlight()
        models = []
        parser = reqparse.RequestParser()
        parser.add_argument("ticket_type")
        parser.add_argument("origin")
        parser.add_argument("destination")
        parser.add_argument("date")
        parser.add_argument("minPrice")
        parser.add_argument("maxPrice")
        self.args = parser.parse_args()
        import multiprocessing
        self.conn = None
        t1 = time.time()
        with multiprocessing.Pool(4) as pool:
            self.models = pool.map(self.mfilter, self.models)
        print(time.time() - t1)
        t1 = time.time()
        self.models = list(filter(None, self.models))
        print(time.time() - t1)
        self.conn = SingletonDB().conn

    def reform(self, row):
        return {"ticket_id": row[0], "seat_number": row[1], "purchase_date": str(row[2]), "user_id": row[3],
                "flight_id": row[4], "departure_time": str(row[5]), "origin": str(row[6]),
                "destination": str(row[7]), "ticket_type": str(row[8]), "price": row[9],
                "is_active": str(row[10])}
