{% snapshot snapshot_session_summary %}

{{
	config(
		target_database='DEV',
		target_schema='analytics',
		unique_key='sessionID',
		strategy='timestamp',
		updated_at='ts'
	)
}}

SELECT * FROM {{ ref('session_summary') }}

{% endsnapshot %}
