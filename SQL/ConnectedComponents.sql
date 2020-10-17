CREATE TABLE nodes (
  paperID INTEGER,
  paperTitle VARCHAR (100));
CREATE TABLE edges (
  paperID INTEGER,
  citedPaperID INTEGER);

DROP PROCEDURE IF EXISTS ConnectedComponents
CREATE PROCEDURE ConnectedComponents
AS BEGIN
    DECLARE @group INT;
    DECLARE @currentID INT;
    DECLARE @count INT;
    DECLARE @subcc TABLE (
        paperID INT,
        Component INT,
        PRIMARY KEY (paperID));
    DECLARE @cc TABLE (
        paperID INT,
        Component INT,
        PRIMARY KEY (paperID));
    DECLARE @remain TABLE (
        paperID INT,
        citedPaperID INT);
    SET @group = 0;
    SET @count = 1;

    INSERT INTO @remain
    SELECT * from edges

    WHILE (@count!=0) BEGIN
        SET @group = @group + 1;
        SET @currentID = (SELECT TOP 1 paperID FROM @remain)

        DELETE FROM @subcc
        WHERE paperID IN (SELECT paperID FROM @subcc)

        INSERT INTO @subcc VALUES(@currentID,@group)

        WHILE (@@ROWCOUNT > 0) BEGIN
            INSERT INTO @subcc
            SELECT DISTINCT [@remain].citedPaperID, @group
            FROM @remain JOIN @subcc
                ON [@remain].paperID = [@subcc].paperID
            WHERE [@remain].citedPaperID NOT IN (SELECT paperID FROM @subcc)
            UNION
            SELECT DISTINCT [@remain].paperID, @group
            FROM @remain JOIN @subcc
                ON [@remain].citedPaperID = [@subcc].paperID
            WHERE [@remain].paperID NOT IN (SELECT paperID FROM @subcc);
        END

        INSERT INTO @cc
        SELECT * FROM @subcc

        DELETE FROM @remain
        WHERE paperID IN (SELECT paperID FROM @subcc) AND citedPaperID IN (SELECT paperID FROM @subcc)

        SET @count = (SELECT COUNT(*) FROM @remain)
    END

    SELECT nodes.paperID, nodes.paperTitle, [@cc].Component
    FROM nodes JOIN @cc ON nodes.paperID = [@cc].paperID
    WHERE [@cc].Component IN (SELECT Component FROM @cc
    GROUP BY Component
    Having COUNT(*) > 4 AND COUNT(*) <= 10)
    ORDER BY [@cc].Component
END;
    GO

EXEC ConnectedComponents;