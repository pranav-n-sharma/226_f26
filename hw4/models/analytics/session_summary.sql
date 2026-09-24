WITH user_session AS (
	SELECT * FROM {{ ref('user_session_channel') }}
),

session_ts AS (
	SELECT * FROM {{ ref('session_timestamp') }}
),

final AS (
	SELECT u.userID, u.sessionID, u.channel, s.ts
	FROM user_session u
	JOIN session_ts s on u.sessionID = s.sessionID
)

SELECT * FROM final
