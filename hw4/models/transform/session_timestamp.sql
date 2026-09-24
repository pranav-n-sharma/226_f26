WITH Source AS (
	SELECT sessionID, ts
	FROM {{ source('raw', 'session_timestamp') }}
	WHERE sessionID IS NOT NULL
)
SELECT *
FROM Source
