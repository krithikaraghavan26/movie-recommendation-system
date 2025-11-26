
DROP VIEW V_MOVIE_AVERAGE_RATING;
DROP TABLE WATCHLIST CASCADE CONSTRAINTS;
DROP TABLE REVIEW_REACTION CASCADE CONSTRAINTS;
DROP TABLE REVIEWS CASCADE CONSTRAINTS;
DROP TABLE USER_PREFERENCE CASCADE CONSTRAINTS;
DROP TABLE USERS CASCADE CONSTRAINTS;
DROP TABLE MOVIES CASCADE CONSTRAINTS;
DROP SEQUENCE reviews_seq;
DROP SEQUENCE preference_seq;
CREATE SEQUENCE reviews_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE preference_seq START WITH 1 INCREMENT BY 1;

-- CREATE TABLES

-- USERS Table
CREATE TABLE USERS (
    user_id     NUMBER(10) PRIMARY KEY,
    name        VARCHAR2(100) NOT NULL,
    email       VARCHAR2(100) UNIQUE NOT NULL
);

-- MOVIES Table
CREATE TABLE MOVIES (
    movie_id      NUMBER(10) PRIMARY KEY,
    title         VARCHAR2(255) NOT NULL,
    genre         VARCHAR2(50) NOT NULL,
    release_year  NUMBER(4),
    director      VARCHAR2(100)
);

-- REVIEWS Table
CREATE TABLE REVIEWS (
    review_id     NUMBER(10) PRIMARY KEY,
    user_id       NUMBER(10) NOT NULL,
    movie_id      NUMBER(10) NOT NULL,
    rating        NUMBER(1) CHECK (rating BETWEEN 1 AND 5),
    review_text   VARCHAR2(4000),
    review_date   DATE,
    
    FOREIGN KEY (user_id) REFERENCES USERS(user_id),
    FOREIGN KEY (movie_id) REFERENCES MOVIES(movie_id),
    CONSTRAINT unique_review UNIQUE (user_id, movie_id) 
);

-- WATCHLIST Table
CREATE TABLE WATCHLIST (
    user_id       NUMBER(10) NOT NULL,
    movie_id      NUMBER(10) NOT NULL,
    added_date    DATE DEFAULT SYSDATE,
    
    PRIMARY KEY (user_id, movie_id),
    FOREIGN KEY (user_id) REFERENCES USERS(user_id),
    FOREIGN KEY (movie_id) REFERENCES MOVIES(movie_id)
);

-- REVIEW_REACTION Table
CREATE TABLE REVIEW_REACTION (
    review_id     NUMBER(10) NOT NULL,
    user_id       NUMBER(10) NOT NULL,
    reaction_type VARCHAR2(10) CHECK (reaction_type IN ('LIKE', 'DISLIKE')),
    
    PRIMARY KEY (review_id, user_id),
    FOREIGN KEY (review_id) REFERENCES REVIEWS(review_id),
    FOREIGN KEY (user_id) REFERENCES USERS(user_id)
);

-- USER_PREFERENCE Table
CREATE TABLE USER_PREFERENCE (
    preference_id NUMBER(10) PRIMARY KEY,
    user_id       NUMBER(10) UNIQUE NOT NULL,
    favorite_genre VARCHAR2(50),
    least_liked_director VARCHAR2(100),
    
    FOREIGN KEY (user_id) REFERENCES USERS(user_id)
);

-- CREATE VIEW
CREATE OR REPLACE VIEW V_MOVIE_AVERAGE_RATING AS
SELECT
    M.movie_id,
    M.title,
    M.genre,
    M.director,
    AVG(R.rating) AS average_rating,
    COUNT(R.review_id) AS total_reviews
FROM MOVIES M
JOIN REVIEWS R ON M.movie_id = R.movie_id
GROUP BY M.movie_id, M.title, M.genre, M.director;