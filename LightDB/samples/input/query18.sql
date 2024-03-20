-- SELECT Sailors.B, SUM(Sailors.A) FROM Sailors, Reserves GROUP BY Sailors.B;
SELECT Sailors.A, SUM(Sailors.A) FROM Sailors GROUP BY Sailors.A;
