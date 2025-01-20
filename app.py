from flask import Flask, jsonify, request
from db_utils import create_tables, insert_data_to_db, get_db_connection, add_payment_amount_data
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Starting Flask application...")

@app.route('/customers', methods=['GET'])
def get_customers():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    offset = (page - 1) * per_page

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM customers LIMIT %s OFFSET %s", (per_page, offset))
    columns = [desc[0] for desc in cur.description]
    customers = []
    for row in cur.fetchall():
        customer = dict(zip(columns, row))
        customers.append(customer)

    cur.close()
    conn.close()

    return jsonify(customers)

@app.route('/subscriptions', methods=['GET'])
def get_subscriptions():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    offset = (page - 1) * per_page

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM subscriptions LIMIT %s OFFSET %s", (per_page, offset))
    columns = [desc[0] for desc in cur.description]
    subscriptions = []
    for row in cur.fetchall():
        subscription = dict(zip(columns, row))
        subscriptions.append(subscription)

    cur.close()
    conn.close()

    return jsonify(subscriptions)


@app.route('/payments', methods=['GET'])
def get_payments():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    offset = (page - 1) * per_page

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM payments LIMIT %s OFFSET %s", (per_page, offset))
    columns = [desc[0] for desc in cur.description]
    payments = []
    for row in cur.fetchall():
        payment = dict(zip(columns, row))
        payments.append(payment)

    cur.close()
    conn.close()

    return jsonify(payments)


@app.route('/usage', methods=['GET'])
def get_usage():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    offset = (page - 1) * per_page

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM usage LIMIT %s OFFSET %s", (per_page, offset))
    columns = [desc[0] for desc in cur.description]
    usage_data = []
    for row in cur.fetchall():
        usage = dict(zip(columns, row))
        usage_data.append(usage)

    cur.close()
    conn.close()

    return jsonify(usage_data)


@app.route('/payment_amount', methods=['GET', 'POST'])
def insert_payment_amount():
    if request.method == 'POST':
        try:
            data = request.get_json()
            if not isinstance(data, list):
                return jsonify({"error": "Payload must be a list of JSON objects"}), 400

            required_fields = {"customer_id", "sum_payment"}
            for entry in data:
                if not isinstance(entry, dict):
                    return jsonify({"error": "Each entry must be a JSON object"}), 400
                if not required_fields.issubset(entry.keys()):
                    return jsonify({"error": f"Each entry must include {', '.join(required_fields)}"}), 400
                if not isinstance(entry["customer_id"], int) or not isinstance(entry["sum_payment"], (int, float)):
                    return jsonify({"error": "Invalid data types. 'customer_id' must be an integer, 'sum_payment' must be a number"}), 400

            conn = get_db_connection()
            cur = conn.cursor()

            insert_query = "INSERT INTO payment_amount (customer_id, sum_payment) VALUES (%s, %s)"
            for entry in data:
                cur.execute(insert_query, (entry["customer_id"], entry["sum_payment"]))

            conn.commit()
            cur.close()
            conn.close()

            return jsonify({"message": "Data inserted successfully"}), 201

        except Exception as e:
            logging.error(f"Error processing request: {e}")
            return jsonify({"error": "Internal server error"}), 500

    elif request.method == 'GET':

        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)
        offset = (page - 1) * per_page

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM payment_amount LIMIT %s OFFSET %s", (per_page, offset))
        columns = [desc[0] for desc in cur.description]
        payment_amount_data = []
        for row in cur.fetchall():
            usage = dict(zip(columns, row))
            payment_amount_data.append(usage)

        cur.close()
        conn.close()

        return jsonify(payment_amount_data)

if __name__ == "__main__":
    create_tables()
    insert_data_to_db()
    add_payment_amount_data()
    app.run(debug=True, host="0.0.0.0", port=4000)
