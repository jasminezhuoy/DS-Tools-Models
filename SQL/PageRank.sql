DROP PROCEDURE IF EXISTS PageRank
CREATE PROCEDURE PageRank
AS
BEGIN
    DECLARE @n INT;
    DECLARE @d FLOAT(25);
    DECLARE @base FLOAT(25);
    DECLARE @diff FLOAT(25);
    DECLARE @total TABLE(
        paperID INT,
        PRIMARY KEY (paperID)
                        );
    DECLARE @pr0 TABLE(
        paperID INT,
        PageRank FLOAT(25),
        PRIMARY KEY (paperID)
                     );
    DECLARE @pr TABLE(
        paperID INT,
        PageRank FLOAT(25),
        PRIMARY KEY (paperID)
                     );
    DECLARE @citation TABLE(
        paperID INT,
        citenum INT,
        PRIMARY KEY (paperID)
                           );
    DECLARE @edges TABLE (
        paperID INT,
        citedPaperID INT
    );

    SET @d = 0.85;

    INSERT INTO @edges
    SELECT * FROM edges

    INSERT INTO @edges
    SELECT n1.paperID, n2.paperID
    from nodes n1, nodes n2
    WHERE NOT EXISTS(
        SELECT *
        FROM edges
        WHERE edges.paperID = n1.paperID
    ) AND n1.paperID <> n2.paperID

    INSERT INTO @total
    SELECT paperID FROM nodes;

    SET @n = (SELECT COUNT(*) FROM @total);
    SET @base = (1.0-@d)/@n;

    INSERT INTO @citation
    SELECT paperID, COUNT(citedPaperID)
    FROM @edges
    GROUP BY paperID;

    INSERT INTO @pr0
    SELECT *, 1.0/@n FROM @total;

    SET @diff = 1.00;

    WHILE (@diff > 0.01) BEGIN

        DELETE @pr WHERE paperID IN (SELECT * FROM @total);

        INSERT INTO @pr
        SELECT [@pr0].paperID, @base+@d*SUM(CASE WHEN ([@citation].citenum IS NULL) THEN 0.0 ELSE @d*[@pr0].PageRank/[@citation].citenum END)
        FROM nodes LEFT OUTER JOIN @edges ON [@edges].citedPaperID = nodes.paperID
            LEFT OUTER JOIN @pr0 ON [@pr0].paperID = [@edges].paperID
            LEFT OUTER JOIN @citation ON [@edges].paperID = [@citation].paperID
        GROUP BY [@pr0].paperID

        SET @diff = (
            SELECT SUM(ABS([@pr].PageRank-[@pr0].PageRank))
            FROM @pr, @pr0
            WHERE [@pr].paperID = [@pr0].paperID
            )

        DELETE @pr0 WHERE paperID IN (SELECT * FROM @total);

        INSERT INTO @pr0
        SELECT * FROM @pr
    END
    SELECT TOP 10 nodes.paperID, nodes.paperTitle, [@pr].PageRank
    FROM nodes JOIN @pr ON nodes.paperID = [@pr].paperID
    ORDER BY [@pr].PageRank DESC
END;
   GO

EXEC PageRank;