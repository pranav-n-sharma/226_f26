WITH Source AS (
	SELECT userID, sessionID, channel
	FROM {{ source('raw','user_session_channel') }}
	WHERE sessionID IS NOT NULL
)
SELECT *
FROM Source
