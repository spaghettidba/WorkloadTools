CREATE TABLE [replay].[Intervals] (
    [interval_id]      INT      NOT NULL,
    [end_time]         DATETIME NOT NULL,
    [duration_minutes] INT      NOT NULL,
    PRIMARY KEY CLUSTERED ([interval_id] ASC)
);

