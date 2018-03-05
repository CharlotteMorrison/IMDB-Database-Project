use imdb;
#add these when calling from java- right after table create.
ALTER TABLE actorroles ADD INDEX (Movie_Identifier); 
ALTER TABLE actorroles ADD INDEX (Name_Identifier);   

SHOW INDEX FROM actorroles;   
#1.  List all the directors who directed a 'Film-Noir' movie in a leap year. (You need to check that the genre is 'Film-Noir' and year is divisible by 4.) Your query should return director name, the movie name, and the year (10 points).

SELECT n.Primary_Name AS DirectorName, m.Primary_Title AS MovieName, m.Start_Year AS Year
	FROM names  n, directors d, movies m, movie_genres mg, genre g
	WHERE g.Genre = 'Film-Noir' 
	AND g.Genre_ID = mg.Genre_ID 
	AND mg.Movie_ID = m.Movie_ID 
	AND d.Movie_Identifier = m.Identifier 
    AND d.Name_Identifier = n.Identifier
    AND m.Start_Year % 4 = 0;
#212 rows returned, took 107 seconds
 

#2.  List the first and last names of all the actors who played in the movie 'Officer 444 (1926)'. (10 points)

SELECT n.Primary_Name 
	FROM movies m, actorroles ar, names n
	WHERE m.Primary_Title = 'Officer \'444\''
    AND ar.Movie_Identifier = m.Identifier
    AND ar.Name_Identifier = n.Identifier ;
#4 rows returned, took 2.453 seconds

#3.  List all the actors who acted in a film before 1900 and also in a film after 2000. (That is: < 1900 and > 2000.) (15 points)
#-----How can this be? Actors can't live more than 100 years, right? You are asked to find the explanation. For that you need to investigate a bit, perhaps run 1-2 additional queries. Once you identify one logical explanation why some actors appear in movies more than 100 years apart, please write it in your turning, as a comment to the SQL query: keep you answer below 1-2 sentences.

SELECT DISTINCT n.Primary_Name
	FROM names n
	where n.Identifier 
    IN (SELECT ar.Name_Identifier 
		FROM actorroles ar, movies m 
        WHERE m.Start_Year < 1900 
        AND ar.Movie_Identifier = m.Identifier)
	AND n.Identifier 
    IN (SELECT ar.Name_Identifier 
		FROM actorroles ar, movies m 
        WHERE m.Start_Year > 2000 
        AND ar.Movie_Identifier = m.Identifier);
#Results:
#Antonio Fino
#Ben Nathan
#Why only 2?
		SELECT Identifier FROM Names WHERE Primary_Name = 'Antonio Fino'; #nm0278321
  		SELECT Identifier FROM Names WHERE Primary_Name = 'Ben Nathan'; #'nm1126916',  

		SELECT m.Primary_Title, m.Start_Year
			FROM movies m, actorroles ar
			WHERE ar.Name_Identifier = 'nm1126916'
			AND ar.Movie_Identifier = m.Identifier;
            
#2 results, 3082 seconds

#4.  List all directors who directed 100 movies or more, in descending order of the number of movies they directed. Return the directors' names and the number of movies each of them directed. (10 points)

SELECT n.Primary_Name, COUNT(*)
	FROM names n, directors d
    WHERE d.Name_Identifier = n.Identifier
    GROUP BY d.Name_Identifier
    HAVING COUNT(*) >= 100
    ORDER BY COUNT(*) DESC;

#returned  over 3967 rows, 868 seconds 

    
#5.  We want to find actors that played two or more roles in the same movie. Notice that Casts may have occasional duplicates, but we are not interested in these: we want actors that had two or more distinct roles in the same movie. (30 points)   
#Write a query that returns the actors' names, the movie name, and the number of distinct roles that they played in that movie (which will be at least 2).
#Write a query that returns the actors' names, the movie name and all the distinct roles (two or more) that the actor played in that movie. 

   
SELECT n.Primary_Name, m.Primary_Title, COUNT(Distinct ar.Character_Role)
	FROM movies m, names n, actorroles ar
	WHERE  n.Identifier = ar.Name_Identifier 
	AND ar.Movie_Identifier = m.Identifier
	GROUP BY n.Identifier, m.Identifier
	HAVING COUNT(Distinct ar.Character_Role) >= 2;
# over 10,000 results, 2087 seconds
    
SELECT n.Primary_Name, m.Primary_Title, ar.Character_Role
	FROM Names n, Movies m, actorroles ar, 
	(SELECT n.Identifier AS thisName, m.Primary_Title AS thisMovie
	FROM Names n, Movies m, actorroles ar
	WHERE n.Identifier = ar.Name_Identifier AND ar.Movie_Identifier = m.Identifier
	GROUP BY n.Identifier, m.Primary_Title 
	HAVING COUNT(*) >= 2) x
WHERE n.Identifier = ar.Name_Identifier and m.Identifier = ar.Movie_Identifier
AND x.thisName = n.Identifier and x.thisMovie = m.Primary_Title;
#over 10,000 results, 7146 seconds    
    

#6.  (a) For each year, count the number of movies in that year that had only female actors. (b) Now make a small change: for each year, report the percentage of movies with only female actors made that year, and also the total number of movies made that year. (30 points)
#For example, one answer will be:
#1990 28.76 6098
#meaning that in 1990 there were 6098 movies, and 28.76% had only female actors.
    
SELECT m.Start_Year, COUNT(*) 
	FROM movies m, actorroles ar, category c 
    WHERE c.Category_Name = 'Actress'
    AND ar.Category_ID = c.Category_ID
    AND ar.Movie_Identifier = m.Identifier
    GROUP BY m.Start_Year ORDER BY m.Start_Year DESC;
    
#it was easier to find all the movies with at least one male than to find the females (for processing consideration)
select m.Start_Year AS Year, 
		CONCAT(FORMAT(((count(*) - male_count)/COUNT(*))*100, 2), '%') AS Percentage, 
        count(*) - male_count AS Female_Only_Cast
	FROM movies m
	INNER JOIN actorroles ar
    ON m.Identifier = ar.Movie_Identifier, 
    (SELECT m.Start_Year AS year, 
		Count(*) AS male_count 
        FROM movies m, actorroles ar 
        WHERE ar.Category_ID = "7" 
        AND m.Identifier = ar.Movie_Identifier 
        GROUP BY m.Start_Year) AS male_set
	WHERE m.Start_Year = male_set.year
	GROUP BY m.start_year;
    
#139 rows returned, 53 seconds

#7.  Find the film(s) with the largest cast. Return the movie title and the size of the cast. By "cast size" we mean the number of distinct actors that played in that movie: if an actor played multiple roles, or if it simply occurs multiple times in casts we still count her/him only once. (10 points)
    
SELECT m.Primary_Title, COUNT(*)
	AS Total_Cast_Size
	FROM Movies m
	INNER JOIN actorroles ar
	ON m.Identifier = ar.Movie_Identifier
	GROUP BY m.identifier, m.Primary_Title
	HAVING COUNT(*) >= ALL (SELECT COUNT(*)
							FROM actorroles
							GROUP BY Movie_Identifier);    
 #1397.56 seconds to run.   

#query to check my count number.
SELECT MAX(actor_count) 
FROM 
	(SELECT COUNT(*) AS actor_count 
    FROM actorroles
	GROUP BY actorroles.Movie_Identifier) 
    AS counts;
    
#other query that returns same results but is slower.
 SELECT Identifier, Primary_Title , total
  FROM (SELECT DISTINCT m.Identifier, m.Primary_Title, count(*) AS total
          FROM Movies AS m, actorroles AS ar
          WHERE ar.Movie_Identifier= m.Identifier
          GROUP BY m.Identifier, m.Primary_Title) as total_count
  WHERE total =
       (SELECT max(total) AS max_count
          FROM (SELECT m.Identifier, count(*) AS total
                FROM Movies AS m, actorroles AS ar
                WHERE ar.Movie_Identifier = m.Identifier GROUP BY m.Identifier) AS total_count);    
                
#8.  A decade is a sequence of 10 consecutive years. For example, 1965, 1966, ..., 1974 is a decade, and so is 1967, 1968, ..., 1976. Find the decade with the largest number of films. (20 points)
#Hint: this query combines several subtleties. One thing you may want to check is that the number of movies in that decade doesn't exceed the total number of movies in the database.

SELECT y.start_year AS decade, SUM(c.total) AS movie_count
FROM 
	(SELECT DISTINCT start_year FROM Movies) AS y, 
	(SELECT start_year, COUNT(*) AS total FROM Movies GROUP BY start_year) AS c 
WHERE c.start_year >= y.start_year  
AND c.start_year < (y.start_year + 10) 
GROUP BY y.start_year 
HAVING SUM(c.total) >= ALL 
	(SELECT SUM(c.total)
		FROM
			(SELECT DISTINCT start_year FROM Movies) AS y,
			(SELECT start_year, COUNT(*) AS total FROM Movies GROUP BY start_year) AS c
		WHERE c.start_year >= y.start_year
		AND c.start_year < (y.start_year + 10)
		GROUP BY y.start_year);

#returns Decade starting in 2008, result 2,262,426


#9.  The Bacon number of an actor is the length of the shortest path between the actor and Kevin Bacon in the "co-acting" graph. That is, Kevin Bacon (KB) has Bacon number 0; all actors who acted in the same film as KB have Bacon number 1; all actors who acted in the same film as some actor with Bacon number 1 have Bacon number 2, etc. Return all actors whose Bacon number is 2. (20 points)
SELECT count(DISTINCT Name_Identifier) 
	FROM actorroles 
    WHERE Movie_Identifier 
    IN (SELECT Movie_Identifier 
		FROM actorroles 
        WHERE Name_Identifier 
        IN (SELECT DISTINCT Name_Identifier 
			FROM actorroles 
            WHERE Movie_Identifier 
            IN (SELECT Movie_Identifier 
				FROM actorroles 
                INNER JOIN Names
				ON Name_Identifier = names.Identifier 
                WHERE Primary_Name = 'Kevin Bacon')))
	AND Name_Identifier 
    NOT IN (SELECT DISTINCT Name_Identifier 
		FROM actorroles 
        WHERE Movie_Identifier 
		IN (SELECT Movie_Identifier 
			FROM actorroles 
            INNER JOIN names 
            ON Name_Identifier = names.Identifier 
            WHERE Primary_Name = 'Kevin Bacon'));
#took 3225.672 seconds: yielded count:16129
        
        
  