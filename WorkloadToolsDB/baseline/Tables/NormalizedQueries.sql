CREATE TABLE [baseline].[NormalizedQueries] (
    [sql_hash]        BIGINT         NOT NULL,
    [normalized_text] NVARCHAR (MAX) NOT NULL,
    [example_text]    NVARCHAR (MAX) NULL,
    PRIMARY KEY CLUSTERED ([sql_hash] ASC)
);

