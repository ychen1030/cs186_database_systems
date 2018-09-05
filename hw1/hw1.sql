DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv, q4v;

-- Question 0
CREATE VIEW q0(era) AS
  SELECT MAX(era)
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear) AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear) AS
  SELECT namefirst, namelast, birthyear
  FROM people
  WHERE namefirst LIKE '% %'
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count) AS
  SELECT birthyear, avg(height), count(*)
  FROM people
  GROUP BY birthyear
  ORDER BY birthyear
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count) AS
  SELECT birthyear, avg(height), count(*)
  FROM people
  GROUP BY birthyear
  HAVING avg(height) > 70
  ORDER BY birthyear
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid) AS
  SELECT p.namefirst, p.namelast, h.playerid, h.yearid
  FROM people AS p, HallOfFame AS h
  WHERE p.playerid = h.playerid AND h.inducted = 'Y'
  ORDER BY h.yearid DESC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid) AS
  SELECT p.namefirst, p.namelast, h.playerid, c.schoolid, h.yearid
  FROM people AS p, HallOfFame AS h, Schools AS s, CollegePlaying AS c
  WHERE p.playerid = h.playerid AND h.inducted = 'Y' AND
        p.playerid = c.playerid AND c.schoolid = s.schoolid AND s.schoolstate = 'CA'
  ORDER BY h.yearid DESC, c.schoolid, h.playerid
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid) AS
  SELECT h.playerid, p.namefirst, p.namelast, c.schoolid
  FROM people AS p, HallOfFame AS h LEFT OUTER JOIN CollegePlaying AS c ON h.playerid = c.playerid
  WHERE p.playerid = h.playerid AND h.inducted = 'Y'
  ORDER BY h.playerid DESC, c.schoolid
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg) AS
  SELECT p.playerid, p.namefirst, p.namelast, b.yearid, CAST(1 AS float)*(h-h2b-h3b-hr + 2*h2b + 3*h3b + 4*hr)/ab AS slg
  FROM People AS p, Batting AS b
  WHERE p.playerid = b.playerid AND b.AB > 50
  ORDER BY slg DESC, b.yearid, b.playerid
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg) AS
  SELECT p.playerid, p.namefirst, p.namelast, CAST(h-h2b-h3b-hr + 2 * h2b + 3 * h3b + 4 * hr AS FLOAT)/ ab as lslg
  FROM People AS p,
      (SELECT playerid, SUM(h), SUM(h2b), SUM(h3b), SUM(hr), SUM(ab)
       From Batting 
       GROUP BY playerid) AS b(playerid, h, h2b, h3b, hr, ab)
  WHERE b.playerid = p.playerid AND b.ab > 50
  ORDER BY lslg DESC, b.playerid
  LIMIT 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg) AS
  SELECT p.namefirst, p.namelast, CAST(b.h-b.h2b-b.h3b-b.hr + 2 * b.h2b + 3 * b.h3b + 4 * b.hr AS FLOAT)/ b.ab as lslg
  FROM People AS p,
      (SELECT playerid, SUM(h), SUM(h2b), SUM(h3b), SUM(hr), SUM(ab)
       From Batting 
       GROUP BY playerid) AS b(playerid, h, h2b, h3b, hr, ab),
      (SELECT playerid, SUM(h), SUM(h2b), SUM(h3b), SUM(hr), SUM(ab)
       From Batting
       WHERE playerid = 'mayswi01'
       GROUP BY playerid) AS m(playerid, h, h2b, h3b, hr, ab)
  WHERE b.playerid = p.playerid AND CAST(b.h-b.h2b-b.h3b-b.hr + 2*b.h2b + 3*b.h3b + 4*b.hr AS FLOAT)/ b.ab > CAST(m.h-m.h2b-m.h3b-m.hr + 2*m.h2b + 3*m.h3b + 4*m.hr AS FLOAT)/m.ab AND b.ab > 50
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev) AS
  SELECT yearid, min(salary), max(salary), avg(salary), stddev(salary)
  FROM Salaries
  GROUP BY yearid
  ORDER BY yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count) AS
WITH RECURSIVE histogram(id, low, high, width) AS
    (SELECT 0 AS id, min, min + (max - min)/10, (max - min)/10
     FROM q4i
     WHERE yearid = 2016
    UNION
     SELECT id + 1, high, CASE WHEN id < 8 THEN high + width ELSE high + width + 1 END, width
     FROM histogram
     WHERE id <= 9)

  SELECT id, low, high, COUNT(*)
  FROM histogram AS h, Salaries AS s
  WHERE salary >= low AND salary < high AND yearid = 2016
  GROUP BY id, low, high
  ORDER BY id
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff) AS
  SELECT b.yearid, b.min - a.min, b.max - a.max, b.avg - a.avg
  FROM q4i AS a, q4i AS b
  WHERE b.yearid = a.yearid + 1
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid) AS
  SELECT p.playerid, namefirst, namelast, salary, yearid
  FROM People As p, Salaries AS s
  WHERE p.playerid = s.playerid AND 
  ((yearid = 2000 AND salary >= (SELECT max(salary) FROM Salaries WHERE yearid = 2000)) OR
  (yearid = 2001 AND salary >= (SELECT max(salary) FROM Salaries WHERE yearid = 2001)))
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT a.teamid, max(s.salary) - min(s.salary)
  FROM AllstarFull AS a, Salaries AS s
  WHERE a.yearid = 2016 AND a.playerid = s.playerid AND s.yearid = 2016
  GROUP BY a.teamid
  ORDER BY a.teamid
;

