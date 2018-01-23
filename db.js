const {Client} = require('pg');

function KchooDB(config) {
	this.client = new Client(config);

	this.client.connect();
}

KchooDB.prototype.addSource = function ({site, id}) {
	return this.client.
		query({
			text: `
				INSERT INTO
					sources (site_id, state, remote_identifier)
				VALUES
					(
						(SELECT id FROM sites WHERE name = $1),
						(SELECT id FROM source_states WHERE name = 'pending'),
						$2
					)
				RETURNING
					id;
			`,
			values: [site, id]
		}).
		then(function ({rows: [{id}]}) {
			return id;
		});
};

KchooDB.prototype.getPendingSources = function ({site}) {
	return this.client.
		query({
			text: `
				UPDATE sources
				SET state = (SELECT id FROM source_states WHERE name = 'populating')
				WHERE id IN
					(
						SELECT id
						FROM sources
						WHERE site_id = (SELECT id FROM sites WHERE name = $2)
							AND state = (SELECT id from source_states WHERE name = 'pending')
						ORDER BY id
						LIMIT 1
					)
				RETURNING id, remote_identifier;
			`,
			values: [state, site, count]
		}).
		then(function ({rows}) {
			return rows.
				map(function ({id: dbID, remote_identifier: twitterID}) {
					return {dbID, twitterID};
				});
		});
};

module.exports = KchooDB;
