-- PL/SQL Function for Movie Recommendation
CREATE OR REPLACE FUNCTION recommend_movies (p_user_id IN NUMBER)
RETURN SYS_REFCURSOR
IS
    c_recommendations SYS_REFCURSOR;
    v_favorite_genre MOVIES.genre%TYPE;
BEGIN
    -- Find the user's favorite genre
    BEGIN
        SELECT genre INTO v_favorite_genre
        FROM (
            SELECT M.genre, AVG(R.rating) AS avg_rating
            FROM REVIEWS R
            JOIN MOVIES M ON R.movie_id = M.movie_id
            WHERE R.user_id = p_user_id
            GROUP BY M.genre
            ORDER BY avg_rating DESC
        ) WHERE ROWNUM = 1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_favorite_genre := 'Action'; 
    END;
    -- Return movies in that genre that the user hasn't reviewed
    OPEN c_recommendations FOR
        SELECT 
            M.movie_id, 
            M.title, 
            M.director, 
            V.average_rating
        FROM MOVIES M
        JOIN V_MOVIE_AVERAGE_RATING V ON M.movie_id = V.movie_id
        WHERE M.genre = v_favorite_genre
        AND M.movie_id NOT IN (SELECT movie_id FROM REVIEWS WHERE user_id = p_user_id)
        ORDER BY V.average_rating DESC, M.release_year DESC
        FETCH NEXT 10 ROWS ONLY;
    RETURN c_recommendations;
END recommend_movies;
/