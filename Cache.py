import datetime
import threading
import multiprocessing as mp

from flask_restful import reqparse

from Database import SingletonDB
from ObjectBuilder import Director, Provider1ObjectBuilder, Provider2ObjectBuilder, OwnObjectBuilder
from psycopg2.extras import execute_values


class SingletonCache(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class CacheProduct(metaclass=SingletonCache):
    def __init__(self):
        self.own_cache = []
        self.service_1_cache = []
        self.service_2_cache = []

    def time_to_update(self):
        dt = datetime.datetime.now()
        tomorrow = dt + datetime.timedelta(days=1)
        return (datetime.datetime.combine(tomorrow, datetime.time.min) - dt).seconds

    def own_prod(self, q):
        director = Director()
        builder = OwnObjectBuilder()
        director.builder = builder
        director.build_all_models()
        own = builder.model
        print(len(own.models))
        q.put(own.models)

    def serv1_prod(self, q):
        director = Director()
        builder = Provider1ObjectBuilder()
        director.builder = builder
        director.build_all_models()
        serv1 = builder.model
        print(len(serv1.models))
        q.put(serv1.models)

    def serv2_prod(self, q):
        director = Director()
        builder = Provider2ObjectBuilder()
        director.builder = builder
        director.build_all_models()
        serv2 = builder.model
        print(len(serv2.models))
        q.put(serv2.models)

    def update(self):
        conn = SingletonDB().conn
        q1 = mp.Queue()
        p1 = mp.Process(target=self.own_prod, args=(q1,))

        q2 = mp.Queue()
        p2 = mp.Process(target=self.serv1_prod, args=(q2,))

        q3 = mp.Queue()
        p3 = mp.Process(target=self.serv2_prod, args=(q3,))
        p1.start()
        p2.start()
        p3.start()
        self.own_cache = q1.get()
        self.service_1_cache = q2.get()
        self.service_2_cache = q3.get()
        for i in range(0, len(self.service_1_cache)):
            self.service_1_cache[i]["ticket_id"] += 150000
        for i in range(0, len(self.service_2_cache)):
            self.service_2_cache[i]["ticket_details_id"] += 151000
        with conn.cursor() as cursor:
            cursor.execute('TRUNCATE ticket_cache')
            execute_values(cursor,
                           '''INSERT INTO ticket_cache ("ticket_id","seat_number", "purchase_date", "ticket_type", "price", "is_active", "user_id", "flight_id","departure_time","destination","origin") VALUES %s''',
                           # ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')''' % (args["product_name"], args["desciption"], str(args["sale_type_id"]), str(args["start_date"]), str(args["end_date"]), str(args["delivery_type_id"]), str(args["delivery_price"]), str(args["delivery_time"]), str(args["price"]),  str(args["seller_id"])))
                           [(self.choose_type_id(args), str(args["seat_number"]),str(args["purchase_date"]),
                             str(args["ticket_type"]),
                             int(float(args["price"])),
                             str(args["is_active"]), args["user_id"], args["flight_id"],
                             str(args["departure_time"]), str(args["destination"]), str(args["origin"]))for args in self.own_cache + self.service_1_cache + self.service_2_cache])
        conn.commit()
        p1.join()
        p2.join()
        p3.join()
        timer = threading.Timer(self.time_to_update(), self.update)
        timer.start()
    def choose_type_id(self,args):
        type_id = ""
        if "ticket_id" in args:
            type_id = args["ticket_id"]
        elif "ticket_details_id" in args:
            type_id = args["ticket_details_id"]
        return type_id
    def get_cache(self):
        parser = reqparse.RequestParser()
        parser.add_argument("ticket_type")
        parser.add_argument("origin")
        parser.add_argument("destination")
        parser.add_argument("date")
        parser.add_argument("minPrice")
        parser.add_argument("maxPrice")
        args = parser.parse_args()
        parse_str = '''SELECT * FROM ticket_cache'''
        filt_opt = []
        if args['ticket_type']:
            filt_opt.append(['"ticket_type"=', args['ticket_type']])
        if args['origin']:
            filt_opt.append(['"origin"=', args['origin']])
        if args['destination']:
            filt_opt.append(['"destination"=', args['destination']])
        if args['date']:
            filt_opt.append(['"departure_time"=', args['date']])
        if args['minPrice']:
            filt_opt.append(['"price">', args['minPrice']])
        if args['maxPrice']:
            filt_opt.append(['"price"<', args['maxPrice']])
        if len(filt_opt) > 0:
            parse_str += ' WHERE '
        for i in range(len(filt_opt)):
            parse_str += filt_opt[i][0] + "'" + filt_opt[i][1] + "'"
            if i + 1 < len(filt_opt):
                parse_str += ' AND '
        conn = SingletonDB().conn
        with conn.cursor() as cursor:
            cursor.execute(parse_str)
            rows = cursor.fetchall()
        result = []
        for row in rows:
            a = {"ticket_id": row[0], "seat_number": row[1],
                 "purchase_date": str(row[2]), "ticket_type": str(row[3]), "price": row[4],
                 "is_active": str(row[5]), "user_id": row[6], "flight_id": row[7], "departure_time": row[8],
                 "origin": row[9], "destination": row[10]}
            result.append(a)
        #print(result)
        return result
