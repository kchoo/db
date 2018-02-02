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

KchooDB.prototype.getPendingSources = function ({site, count = 1}) {
	return this.client.
		query({
			text: `
				UPDATE sources
				SET state = 2
				WHERE id IN
					(
						SELECT id
						FROM sources
						WHERE site_id = (SELECT id FROM sites WHERE name = $1::text)
						AND state = 1 OR state = 2
						-- ordering by id should mean that sources which didn't finish
						-- populating in one go will be processed first
						-- (although it doesn't matter since it's all done in parallel)
						ORDER BY id
						LIMIT $2::integer
					)
				RETURNING id, remote_identifier;
			`,
			values: [site, count]
		}).
		then(function ({rows}) {
			return rows.
				map(function ({id: dbID, remote_identifier: twitterID}) {
					return {dbID, twitterID};
				});
		});
};

// TODO: use proper binding syntax for updateLatest
// i.e. find a way to make a no-op if latestID isn't set
KchooDB.prototype.saveSource = function ({
	id,
	earliestID,
	latestID = undefined
}) {
	let updateLatest = '';

	if (latestID) {
		updateLatest = `, latest_processed_id = ${latestID}`;
	}

	return this.client.
		query({
			text: `
				UPDATE sources
				SET
					earliest_processed_id = $1
					${updateLatest}
				WHERE id = $2
			`,
			values: [earliestID, id]
		});
};

KchooDB.prototype.setErrors = function (ids) {
	if (ids.length === 0) {
		return Promise.resolve('No sources errored');
	}

	const sourceIDString = ids.join(',');

	return this.client.
		query({
			text: `
				UPDATE sources
				SET state = (SELECT id FROM source_states WHERE name = 'error')
				WHERE id IN (${sourceIDString})
			`,
			values: []
		}).
		then(function ({rowCount}) {
			return `${rowCount} sources errored: ${sourceIDString}`;
		});
};

// TODO: use proper binding syntax for sources string
// i.e. figure out how node-postgres does arrays
// and convert that for the IN clause
KchooDB.prototype.finishPopulatingSources = function (sources) {
	if (sources.length === 0) {
		return Promise.resolve('No sources finished populating');
	}

	const sourceIDString = sources.join(',');

	return this.client.
		query({
			text: `
				UPDATE sources
				SET state = (SELECT id FROM source_states WHERE name = 'standby')
				WHERE id IN (${sourceIDString})
			`,
			values: []
		}).
		then(function ({rowCount}) {
			return `${rowCount} sources populated: ${sourceIDString}`;
		});
};

KchooDB.prototype.saveImages = function ({sourceID, urls}) {
	if (urls.length === 0) {
		return Promise.resolve('No images inserted');
	}

	// (1,'url'),(1,'url'),...
	const urlString = urls.
		map(el => `(${sourceID}, '${el}')`).
		join(',');

	return this.client.
		query({
			text: `
				INSERT INTO images (source_id, source_url) VALUES ${urlString} ON CONFLICT DO NOTHING
			`,
			values: []
		}).
		then(function ({rowCount}) {
			return `${rowCount} images inserted: ${urls.join(',')}`;
		});
};

// TODO: figure out how many images one invocation can process
// and set default count based on that
KchooDB.prototype.getImagesToDownload = function () {
	return this.client.
		query({
			text: `
				SELECT id, source_id, source_url
				FROM images
				WHERE s3_url IS NULL
			`,
			values: []
		}).
		then(function ({rows}) {
			return rows.
				map(function ({id, source_url: sourceURL, source_id: sourceID}) {
					return {id, sourceURL, sourceID};
				});
		});
};

KchooDB.prototype.updateImage = function ({id, url}) {
	return this.client.
		query({
			text: `
				UPDATE images SET s3_url = $2 WHERE id = $1
			`,
			values: [id, url]
		}).
		then(function () {
			return `Uploaded image ${id} to S3`;
		});
};

module.exports = KchooDB;
