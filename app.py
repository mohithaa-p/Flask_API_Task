from flask import Flask, request, jsonify
import logging
import json
from database import init_db, get_connection
app=Flask(__name__)
DEFAULT_PAGE=1
DEFAULT_LIMIT=5
logging.basicConfig(level=logging.INFO)
init_db()
def row_to_dict(row):
    return dict(row)
@app.route("/")
def home():
    return {"message": "CRUD Operations"}

@app.route("/load-data", methods=["POST"])
def load_data():
    try:
        with open("users.json", "r") as file:
            users = json.load(file)
        conn = get_connection()
        cursor = conn.cursor()
        for user in users:
            cursor.execute("""
                INSERT INTO users 
                (first_name, last_name, company_name, age, city, state, zip, email, web)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user["first_name"],
                user["last_name"],
                user.get("company_name"),
                user.get("age"),
                user.get("city"),
                user.get("state"),
                user.get("zip"),
                user.get("email"),
                user.get("web")
            ))
        conn.commit()
        conn.close()
        return {"message": "Data loaded successfully"}, 201

    except Exception as e:
        logging.error(str(e))
        return {"error": str(e)}, 500

@app.route("/api/users",methods=["GET"])
def get_users():
    page=int(request.args.get("page",DEFAULT_PAGE))
    limit=int(request.args.get("limit",DEFAULT_LIMIT))
    search=request.args.get("search")
    sort=request.args.get("sort")
    city=request.args.get("city")
    offset=(page-1)*limit
    query="SELECT * FROM users WHERE 1=1"
    params=[]
    if search:
        query += " AND (LOWER(first_name) LIKE ? OR LOWER(last_name) LIKE ?)"
        params.extend([f"%{search.lower()}%", f"%{search.lower()}%"])
    if city:
        query+= " AND LOWER(city) LIKE ?"
        params.append(f"%{city.lower()}%")
    if sort:
        direction="ASC"
        field=sort
        if sort.startswith("-"):
            direction="DESC"
            field=sort[1:]
        allowed_fields=["id","first_name","last_name","age","city"]
        if field in allowed_fields:
            query+=f" ORDER BY {field} {direction}"
    query+=" LIMIT ? OFFSET ?"
    params.extend([limit,offset])
    conn=get_connection()
    users=conn.execute(query,params).fetchall()
    conn.close()
    return jsonify([row_to_dict(user) for user in users])

@app.route("/api/users/<int:user_id>", methods=["GET"])
def get_user(user_id):
    conn=get_connection()
    user=conn.execute("SELECT * FROM users WHERE id= ?",(user_id,)).fetchone()
    conn.close()
    if user:
        return jsonify(row_to_dict(user))
    return{"error":"User not found"},404

@app.route("/api/users", methods=["POST"])
def create_user():
    data=request.json
    conn=get_connection()
    cursor=conn.cursor()
    cursor.execute("""
                   insert into users(
                   first_name,last_name,company_name, age, city, state, zip, email,web)
                   VALUES(?,?,?,?,?,?,?,?,?)""", (
                       data["first_name"],
                       data["last_name"],
                       data.get("company_name"),
                       data.get("age"),
                       data.get("city"),
                       data.get("state"),
                       data.get("zip"),
                       data.get("email"),
                       data.get("web")
                   ))
    conn.commit()
    user_id=cursor.lastrowid
    conn.close()
    return {"id":user_id, "message": "User created"},201

@app.route("/api/users/<int:user_id>", methods=["PUT"])
def update_user(user_id):
    data = request.json
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(""" UPDATE users SET
            first_name = ?,
            last_name = ?,
            company_name = ?,
            age = ?,
            city = ?,
            state = ?,
            zip = ?,
            email = ?,
            web = ?
        WHERE id = ?
    """, (
        data["first_name"],
        data["last_name"],
        data.get("company_name"),
        data.get("age"),
        data.get("city"),
        data.get("state"),
        data.get("zip"),
        data.get("email"),
        data.get("web"),
        user_id
    ))
    conn.commit()
    conn.close()
    return {"message": "User updated"}

@app.route("/api/users/<int:user_id>", methods=["PATCH"])
def patch_user(user_id):
    data = request.json
    fields = []
    values = []
    for key, value in data.items():
        fields.append(f"{key} = ?")
        values.append(value)
    values.append(user_id)
    query = f"UPDATE users SET {', '.join(fields)} WHERE id = ?"
    conn = get_connection()
    conn.execute(query, values)
    conn.commit()
    conn.close()
    return {"message": "User partially updated"}

@app.route("/api/users/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    conn = get_connection()
    conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
    return {"message": "User deleted"}

@app.route("/api/users/summary", methods=["GET"])
def summary():
    conn = get_connection()
    city_stats = conn.execute("""
        SELECT city, COUNT(*) as count
        FROM users
        GROUP BY city
    """).fetchall()
    avg_age = conn.execute("""
        SELECT AVG(age) FROM users
    """).fetchone()[0]
    conn.close()
    return jsonify({
        "count_by_city": [dict(row) for row in city_stats],
        "average_age": avg_age
    })

if __name__ == "__main__":
    app.run(debug=True)



