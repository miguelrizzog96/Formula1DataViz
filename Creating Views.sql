USE f1db;
CREATE VIEW dataset AS
SELECT r.name, CONCAT(forename,' ',surname) AS driver ,lap, milliseconds, year
FROM lapTimes lt
JOIN races r ON r.raceid=lt.raceid
JOIN drivers d ON d.driverid=lt.driverid
WHERE year >=2021;

CREATE VIEW DriverLineups AS
SELECT Distinct c.name ,CONCAT(forename,' ',surname) AS Driver, year  FROM constructors c
JOIN results re ON re.constructorId=c.constructorId
JOIN races r ON r.raceId=re.raceId
Join drivers d On re.driverId=d.driverId
ORDER BY  year ,c.name;


-- Boxplots Project Dataset
SELECT d.*, l.name AS team 
FROM dataset d 
JOIN driverlineups l ON l.Driver=d.Driver AND l.year=d.year
ORDER BY name, Team, Driver, lap;

-- Laptimes Project Dataset
-- SELECT  Driver_name ,  Laptime, Race, Season, Team, Lap, Pos

SELECT r.year AS Season ,r.round, r.name AS Race, CONCAT(d.forename," ",d.surname) AS Driver_name , ln.name AS Team, lt.lap, lt.time AS Laptime , lt.milliseconds , lt.position AS Pos
FROM drivers d
JOIN laptimes lt ON lt.driverId=d.driverId
JOIN races r ON lt.raceId=r.raceId
JOIN DriverLineups ln ON CONCAT(d.forename," ",d.surname)=ln.Driver AND r.year =ln.year
Where r.year >=2021
ORDER BY r.year ASC, r.round ASC, lt.Lap ASC;

-- Season domination view
SELECT SUM(AvailablePoints), MAX(Races), year
FROM	
    (SELECT
		year,
		MAX(MAX_points),
		MAX(n_races) AS Races,
		"Race"  AS  type ,
		CASE
		WHEN year = 2014 THEN (25 *(MAX(n_races)-1)) +Max(MAX_points)
		ELSE MAX(MAX_points)*MAX(n_races) END AS AvailablePoints
	FROM(
		SELECT
			r.year,
			MAX(re.points) as MAX_Points ,
			COUNT(*) OVER (PARTITION BY r.year) AS n_races
		FROM 
			results re
		JOIN
			races r ON re.raceId=r.raceId
		GROUP BY 
			r.year, r.raceId
		) t1
	GROUP BY
		year

	
	ORDER BY 
		YEAR DESC,
		Type) t3
GROUP BY 
	year;


SELECT 
	t1.raceId,t1.name, t1.year, t2.LastRound,t3.p
FROM
	races t1
JOIN
	(SELECT 
		MAX(round) AS LastRound,
		year
	FROM 
		races r1
	GROUP BY 
		year) t2 ON  t1.year=t2.year AND t2.LastRound=t1.Round
JOIN
	(SELECT raceId, Max(Points) AS p
	FROM driverStandings
	GROUP BY raceId,driverId)t3  ON t3.raceId=t1.raceId
ORDER BY 
	year DESC
;
-- times between a bad pitstops query badstop/totalStops
SELECT
    r.year,
    r.name AS race,
    c.name AS Team,
    pits.milliseconds as PitStop
FROM pitStops pits
JOIN races r ON r.raceId=pits.raceId
JOIN (SELECT DISTINCT raceid,constructorId,driverId FROM Results) re ON
re.raceId=pits.raceId AND pits.driverId=re.driverId
JOIN constructors c ON c.constructorId=re.constructorId
ORDER BY r.year DESC, r.round




 
