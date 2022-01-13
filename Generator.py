import random
import psycopg2


def random_gen(id):
    ticket_details_id = id
    seat_number = random.randint(1, 350)
    purchase_date = "2021-" + str(random.randint(1, 12)) + "-" + str(random.randint(1, 27))
    user_id = random.randint(1, 15)
    flight_id = random.randint(1, 13)
    choose_var = ["econom", "business"]
    ticket_type = random.choice(choose_var)
    price = random.randint(1000, 3500)
    is_active = False
    randomBool = random.randint(0, 1)
    if randomBool == 0:
        is_active = False
    else:
        is_active = True


    return {
        "ticket_details_id": ticket_details_id, "seat_number": seat_number, "purchase_date": purchase_date, "user_id": user_id, "flight_id": flight_id,
        "ticket_type": ticket_type, "is_active": is_active, "price": price
    }


def select_filtered_values1():
    conn = psycopg2.connect(dbname='FlightLabKpi2', user='postgres', password='0672089596', host='localhost')
    rows = []
    for i in range(0, 100000):
        args = random_gen(i + 1)
        with conn.cursor() as cursor:
            cursor.execute(
                '''INSERT INTO "FlightLabKpi2".public."public.Ticket"("seat_number", "purchase_date", "ticket_type", "price", "is_active", "user_id", "flight_id") VALUES(%s,'%s','%s',%s,%s,%s,%s)''' % (
                    str(args["seat_number"]), str(args["purchase_date"]), str(args["ticket_type"]),
                    str(args["price"]),
                    str(args["is_active"]),
                    str(args["user_id"]), str(args["flight_id"])))
    conn.commit()
    conn.close()


def select_filtered_values2():
    conn = psycopg2.connect(dbname='model2_2', user='postgres', password='0672089596', host='localhost')
    rows = []
    price_arr = []
    for i in range(0, 50000):
        args = random_gen(i + 1)
        with conn.cursor() as cursor:
            cursor.execute(
                '''INSERT INTO model2_2.public."public.TicketDetails"("seat_number", "purchase_date", "user_id", "flight_id") VALUES(%s,'%s',%s,%s)''' % (
                    str(args["seat_number"]), str(args["purchase_date"]), str(args["user_id"]),
                    str(args["flight_id"])))

        price_arr.append({"ticket_details_id": str(i + 1), "ticket_type": str(args["ticket_type"]), "price": str(args["price"]), "is_active": str(args["is_active"])})

    conn.commit()
    conn.close()

    conn = psycopg2.connect(dbname='model2_2', user='postgres', password='0672089596', host='localhost')
    for i in range(0, 50000):
        with conn.cursor() as cursor:
            cursor.execute(
                '''INSERT INTO model2_2.public."public.TicketMainInfo" ("ticket_details_id", "ticket_type", "price","is_active") VALUES(%s,'%s',%s,%s)''' % (
                    str(i + 1), str(price_arr[i]["ticket_type"]), str(price_arr[i]["price"]),
                    str(price_arr[i]["is_active"])))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    select_filtered_values1()
    select_filtered_values2()
