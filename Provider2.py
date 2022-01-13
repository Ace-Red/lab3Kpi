from flask import Flask
from flask_restful import Resource, Api, reqparse
import psycopg2
import copy


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class SingletonDB(metaclass=SingletonMeta):
    def __init__(self):
        self.conn = psycopg2.connect(dbname='model2_2', user='postgres', password='0672089596', host='localhost')

    def select_all_price(self, page):
        rows = []
        with self.conn.cursor() as cursor:
            cursor.execute(
                'SELECT model2_2.public."public.TicketMainInfo"."ticket_id", model2_2.public."public.TicketMainInfo"."ticket_type", model2_2.public."public.TicketMainInfo"."price" , model2_2.public."public.TicketMainInfo"."is_active" FROM model2_2.public."public.TicketMainInfo" INNER JOIN model2_2.public."public.TicketDetails" ON model2_2.public."public.TicketMainInfo"."ticket_id" = model2_2.public."public.TicketDetails"."ticket_details_id" LIMIT 5000 OFFSET ' + str(
                    (page - 1) * 5000))
            rows = cursor.fetchall()
        return rows

    def select_all_desc(self, i):
        rows = []
        with self.conn.cursor() as cursor:
            cursor.execute(
                'SELECT model2_2.public."public.TicketDetails"."ticket_details_id",  model2_2.public."public.TicketDetails"."seat_number", model2_2.public."public.Flight"."departure_time", model2_2.public."public.TicketDetails"."purchase_date", model2_2.public."public.TicketDetails"."user_id", model2_2.public."public.TicketDetails"."flight_id", model2_2.public."public.TicketMainInfo"."ticket_type", model2_2.public."public.TicketMainInfo"."price", model2_2.public."public.TicketMainInfo"."is_active",model2_2.public."public.Flight"."origin",model2_2.public."public.Flight"."destination" FROM model2_2.public."public.TicketDetails" INNER JOIN model2_2.public."public.TicketMainInfo" on model2_2.public."public.TicketDetails"."ticket_details_id" = model2_2.public."public.TicketMainInfo"."ticket_details_id" INNER JOIN model2_2.public."public.Flight" on model2_2.public."public.TicketDetails"."flight_id" = model2_2.public."public.Flight"."flight_id" WHERE model2_2.public."public.TicketDetails"."ticket_details_id"=%d' % i)
            rows = cursor.fetchall()
        return rows


class GetPrices(Resource):
    def get(self):
        db = SingletonDB()
        parser = reqparse.RequestParser()
        parser.add_argument("page")
        args = parser.parse_args()
        page = 1
        if args['page']:
            page = int(args['page'])
        all_models = db.select_all_price(page)
        my_list = []
        for row in all_models:
            a = {"ticket_id": str(row[0]), "ticket_type": str(row[1]), "price": str(row[2]), "is_active": str(row[3])}
            my_list.append(a)
        return my_list


class GetDescription(Resource):
    def get(self, id):
        db = SingletonDB()
        all_models = db.select_all_desc(id)
        my_list = []
        for row in all_models:
            a = {"ticket_details_id": row[0], "seat_number": row[1],"departure_time":str(row[2]), "purchase_date": str(row[3]),
                 "user_id": row[4],
                 "flight_id": row[5], "ticket_type": str(row[6]), "price": row[7],
                 "is_active": str(row[8]), "origin": str(row[9]), "destination": str(row[10])
                 }
            my_list.append(a)

        return my_list[0]


if __name__ == "__main__":
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(GetPrices, '/price-list/')
    api.add_resource(GetDescription, '/details/<int:id>')
    app.run(port=5002, debug=True)
