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

KchooDB.prototype.getSourcesToPopulate = function ({site, count = 1}) {
	return this.client.
		query({
			text: `
				UPDATE sources
				SET state = (SELECT id FROM source_states WHERE name = 'populating')
				WHERE id IN
					(
						SELECT id
						FROM sources
						WHERE site_id = (SELECT id FROM sites WHERE name = $1::text)
						AND state = (SELECT id FROM source_states WHERE name = 'pending')
							OR state = (SELECT id FROM source_states WHERE name = 'populating')
						-- ordering by id should mean that sources which didn't finish
						-- populating in one go will be processed first
						-- (although it doesn't matter since it's all done in parallel)
						ORDER BY id
						LIMIT $2::integer
					)
				RETURNING id, remote_identifier, earliest_processed_id;
			`,
			values: [site, count]
		}).
		then(function ({rows}) {
			return rows.
				map(function ({
					id: dbID,
					remote_identifier: twitterID,
					earliest_processed_id: earliest
				}) {
					return {dbID, twitterID, earliest};
				});
		});
};

KchooDB.prototype.getSourcesToRefresh = function () {
	return this.client.
		query({
			text: `
				UPDATE sources
				SET state = (SELECT id FROM source_states WHERE name = 'refreshing')
				WHERE id IN
					(
						SELECT id
						FROM sources
						WHERE state = (SELECT id FROM source_states WHERE name = 'standby')
						ORDER BY last_refreshed NULLS FIRST
					)
				RETURNING id, remote_identifier, latest_processed_id;
			`,
			values: []
		}).
		then(function ({rows}) {
			return rows.
				map(function ({
					id: dbID,
					remote_identifier: twitterID,
					latest_processed_id: latest
				}) {
					return {dbID, twitterID, latest};
				});
		});
};

// TODO: use proper binding syntax for updateLatest
// i.e. find a way to make a no-op if latestID isn't set
KchooDB.prototype.saveSource = function ({
	id,
	earliestID = undefined,
	latestID = undefined
}) {
	const updateSet = [];

	if (latestID) {
		updateSet.push(`latest_processed_id = ${latestID}`);
	}
	if (earliestID) {
		updateSet.push(`earliest_processed_id = ${earliestID}`);
	}

	const set = updateSet.join(',')

	return this.client.
		query({
			text: `
				UPDATE sources
				SET ${updateSet}
				WHERE id = $1
			`,
			values: [id]
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
KchooDB.prototype.finishProcessingSources = function (sources, action) {
	if (sources.length === 0) {
		return Promise.resolve(`0 source(s) finished ${action}`);
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
			return `${rowCount} source(s) finished ${action}: ${sourceIDString}`;
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
