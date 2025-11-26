from flask import Flask, render_template, request, redirect, url_for, flash
import cx_Oracle
from datetime import datetime
from flask_login import LoginManager, UserMixin, login_user, logout_user, current_user, login_required
from werkzeug.security import check_password_hash

app = Flask(__name__)
app.config['SECRET_KEY'] = 'a_very_secret_and_long_key_for_sessions' 
# --- ORACLE CONNECTION CONFIGURATION ---
ORACLE_USER = "system"
ORACLE_PASS = "root"
ORACLE_HOST = "localhost" 
ORACLE_PORT = "1521"       
ORACLE_SERVICE = "xepdb1" 
DSN = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, service_name=ORACLE_SERVICE)
def get_db_connection():
    """Establishes and returns an Oracle database connection."""
    try:
        connection = cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=DSN)
        return connection
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None
# ----------------------------------------------------------------------
# --- FLASK-LOGIN SETUP ---
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' 
class User(UserMixin):
    def __init__(self, user_id, name, email, password_hash):
        self.id = user_id
        self.name = name
        self.email = email
        self.password_hash = password_hash

@login_manager.user_loader
def load_user(user_id):
    conn = get_db_connection()
    if not conn:
        return None
    cursor = conn.cursor()
    user = None
    try:
        cursor.execute("SELECT user_id, name, email, password_hash FROM USERS WHERE user_id = :p_id", p_id=user_id)
        row = cursor.fetchone()
        if row:
            user = User(row[0], row[1], row[2], row[3])
    except Exception as e:
        print(f"Error loading user: {e}")
    cursor.close()
    conn.close()
    return user
# ----------------------------------------------------------------------
# --- AUTHENTICATION ROUTES ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        conn = get_db_connection()
        if not conn:
            flash("Database connection error.", 'danger')
            return redirect(url_for('login'))
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT user_id, name, email, password_hash FROM USERS WHERE email = :p_email", p_email=email)
            row = cursor.fetchone()
            if row:
                user = User(row[0], row[1], row[2], row[3])
                if check_password_hash(user.password_hash, password):
                    login_user(user) 
                    flash('Logged in successfully!', 'success')
                    next_page = request.args.get('next')
                    return redirect(next_page or url_for('index'))
                else:
                    flash('Invalid password.', 'danger')
            else:
                flash('Invalid email or password.', 'danger') 
        except Exception as e:
            flash(f'An error occurred during login: {e}', 'danger')
        finally:
            cursor.close()
            conn.close()
    return render_template('login.html')
@app.route('/logout')
@login_required 
def logout():
    logout_user()
    flash('You have been logged out.', 'success')
    return redirect(url_for('login'))
# ----------------------------------------------------------------------
# --- PROTECTED FLASK ROUTES ---
@app.route('/')
@login_required 
def index():
    user_id = current_user.id 
    user_name = current_user.name
    conn = get_db_connection()
    if not conn:
        return "Database Connection Error", 500
    cursor = conn.cursor()
    top_movies = []
    try:
        cursor.execute("""
            SELECT title, genre, director, ROUND(average_rating, 2)
            FROM V_MOVIE_AVERAGE_RATING
            ORDER BY average_rating DESC
            FETCH NEXT 10 ROWS ONLY
        """)
        top_movies = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching top movies: {e}")
    recommendations = []
    try:
        recommendation_cursor = cursor.callfunc("recommend_movies", cx_Oracle.CURSOR, [user_id])
        recommendations = recommendation_cursor.fetchall()
    except Exception as e:
        print(f"Error fetching recommendations: {e}")
    cursor.close()
    conn.close()
    return render_template(
        'index.html', 
        user_id=user_id,
        user_name=user_name,
        top_movies=top_movies,
        recommendations=recommendations
    )
@app.route('/review/<int:movie_id>')
@login_required 
def movie_detail(movie_id):
    user_name = current_user.name
    conn = get_db_connection()
    if not conn:
        return "Database Connection Error", 500
    cursor = conn.cursor()
    movie_info = None
    try:
        cursor.execute("""
            SELECT M.title, M.director, M.release_year, M.genre, ROUND(V.average_rating, 2)
            FROM MOVIES M LEFT JOIN V_MOVIE_AVERAGE_RATING V ON M.movie_id = V.movie_id
            WHERE M.movie_id = :p_mid
        """, p_mid=movie_id)
        movie_info = cursor.fetchone()
    except Exception as e:
        print(f"Error fetching movie info: {e}")
    if not movie_info:
        cursor.close()
        conn.close()
        return "Movie Not Found", 404
    reviews = []
    try:
        cursor.execute("""
            SELECT U.name, R.rating, R.review_text, TO_CHAR(R.review_date, 'DD-MON-YYYY')
            FROM REVIEWS R
            JOIN USERS U ON R.user_id = U.user_id
            WHERE R.movie_id = :p_mid
            ORDER BY R.review_date DESC
        """, p_mid=movie_id)
        reviews = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching reviews: {e}")
    cursor.close()
    conn.close()
    return render_template(
        'review.html',
        movie_id=movie_id,
        movie_info=movie_info,
        reviews=reviews,
        user_name=user_name
    )
@app.route('/submit_review', methods=['POST'])
@login_required 
def submit_review():
    user_id = current_user.id 
    movie_id = request.form.get('movie_id')
    rating = request.form.get('rating')
    review_text = request.form.get('review_text')
    conn = get_db_connection()
    if not conn:
        return "Database Connection Error", 500
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM REVIEWS WHERE user_id = :p_uid AND movie_id = :p_mid",
                        p_uid=user_id, p_mid=movie_id)
        if cursor.fetchone()[0] > 0:
            flash(f"Error: You have already reviewed this movie.", 'danger')
            return redirect(url_for('movie_detail', movie_id=movie_id))
        cursor.execute("""
            INSERT INTO REVIEWS (review_id, user_id, movie_id, rating, review_text, review_date)
            VALUES (reviews_seq.NEXTVAL, :p_uid, :p_mid, :p_rating, :p_text, SYSDATE)
        """, p_uid=user_id, p_mid=movie_id, p_rating=rating, p_text=review_text)
        conn.commit()
        flash("Review submitted successfully!", 'success')
    except Exception as e:
        conn.rollback()
        error_message = str(e)
        print(f"Error submitting review: {error_message}")
        if 'ORA-02291' in error_message:
            flash(f"Error: Movie or User ID not found. Data load issue.", 'danger')
        else:
            flash(f"Database Error: {error_message}", 'danger')
    finally:
        cursor.close()
        conn.close()
    return redirect(url_for('movie_detail', movie_id=movie_id))
@app.route('/watchlist', methods=['GET', 'POST'])
@login_required 
def watchlist():
    user_id = current_user.id
    user_name = current_user.name
    conn = get_db_connection()
    if not conn:
        flash("Database Connection Error.", 'danger')
        return redirect(url_for('index'))
    cursor = conn.cursor()
    if request.method == 'POST':
        action = request.form.get('action')
        movie_id = request.form.get('movie_id')
        print(f"DEBUG: Attempting to {action} movie {movie_id} for user {user_id}") 
        try:
            if action == 'add':
                cursor.execute("""
                    INSERT INTO WATCHLIST (user_id, movie_id, added_date)
                    VALUES (:p_user_id, :p_movie_id, SYSDATE)
                """, p_user_id=user_id, p_movie_id=movie_id) 
                flash("Movie added to watchlist!", 'success')
            elif action == 'remove':
                cursor.execute("""
                    DELETE FROM WATCHLIST WHERE user_id = :p_user_id AND movie_id = :p_movie_id
                """, p_user_id=user_id, p_movie_id=movie_id) 
                flash("Movie removed from watchlist.", 'info')
            conn.commit()
        except Exception as e:
            conn.rollback()
            error_message = str(e)
            if 'ORA-00001' in error_message:
                flash("This movie is already in your watchlist.", 'warning')
            elif 'ORA-02291' in error_message: 
                print(f"\n--- FATAL WATCHLIST ERROR (ORA-02291) ---")
                print(f"Error: Foreign Key Violation. User ID or Movie ID does not exist in parent table.")
                print(f"Error Details: {error_message}\n")
                flash(f"Error adding to watchlist: Movie or User ID not found.", 'danger')
            else:
                print(f"--- UNEXPECTED WATCHLIST ERROR ---")
                print(f"Watchlist operation error: {error_message}")
                print(f"----------------------------------")
                flash(f"Database Error: {error_message}", 'danger')
    watchlist_items = []
    try:
        cursor.execute("""
            SELECT M.movie_id, M.title, M.director, M.genre
            FROM WATCHLIST W
            JOIN MOVIES M ON W.movie_id = M.movie_id
            WHERE W.user_id = :p_user_id
            ORDER BY W.added_date DESC
        """, p_user_id=user_id) 
        watchlist_items = cursor.fetchall()
    except Exception as e:
        print(f"Error fetching watchlist: {e}")
    cursor.close()
    conn.close()
    return render_template(
        'watchlist.html', 
        user_name=user_name,
        watchlist_items=watchlist_items
    )
if __name__ == '__main__':
    app.run(debug=True)