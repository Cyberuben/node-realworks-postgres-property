const pg = require("pg");
const escape = require("sql-template-strings");

function parseProperty(property) {
	var returnData = {};

	if(property.hasOwnProperty("Wonen")) {
		returnData.type = "wonen";

		if(property.Wonen.hasOwnProperty("Woonhuis")) {
			returnData.propertyType = "woonhuis";
		}else if(property.Wonen.hasOwnProperty("Appartement")) {
			returnData.propertyType = "appartement";
		}else{
			returnData.propertyType = "anders";
		}

		var address;
		if(property.ObjectDetails.Adres.hasOwnProperty("Nederlands")) {
			address = property.ObjectDetails.Adres.Nederlands;

			returnData.street = address.Straatnaam;
			returnData.number = address.Huisnummer;
			if(address.hasOwnProperty("HuisnummerToevoeging")) {
				returnData.numberAddition = address.HuisnummerToevoeging;
			}else{
				returnData.numberAddition = null;
			}
			returnData.postcode = address.Postcode;
			returnData.city = address.Woonplaats;
			returnData.country = address.Land;
		}else{
			address = property.ObjectDetails.Adres.Internationaal;

			returnData.street = address.Adresregel1;
			if(address.hasOwnProperty("Adresregel2")) {
				returnData.street += " " + address.Adresregel2;
			}
			returnData.number = null;
			returnData.numberAddition = null;
			returnData.postcode = null;
			returnData.city = address.Woonplaats;
			returnData.country = address.Land;
		}

		returnData.objectStatus = property.ObjectDetails.StatusBeschikbaarheid.Status;
	}else if(property.hasOwnProperty("Gebouw")) {
		returnData.type = "bog";

		if(property.Gebouw.hasOwnProperty("Winkelruimte")) {
			returnData.propertyType = "winkelruimte";
		}else if(property.Gebouw.hasOwnProperty("Bedrijfsruimte")) {
			returnData.propertyType = "bedrijfsruimte";
		}else{
			returnData.propertyType = "anders";
		}

		var address = property.ObjectDetails.Adres;
		returnData.street = address.Straatnaam;
		returnData.number = address.Huisnummer.Hoofdnummer;
		if(address.hasOwnProperty("HuisnummerToevoeging")) {
			returnData.numberAddition = address.HuisnummerToevoeging;
		}else{
			returnData.numberAddition = null;
		}
		returnData.postcode = address.Postcode;
		returnData.city = address.Woonplaats;
		returnData.country = null;

		returnData.objectStatus = property.ObjectDetails.Status.StatusType;
	}else{
		returnData.type = "other";
	}

	if(property.ObjectDetails.hasOwnProperty("Koop")) {
		returnData.buy = true;

		returnData.buyPrefix = property.ObjectDetails.Koop.Prijsvoorvoegsel;
		returnData.buySuffix = "";

		if(returnData.buyPrefix == "prijs op aanvraag") {
			returnData.buyPrefix = "Vraagprijs";
			returnData.buySuffix = "op aanvraag";
		}else{
			if(returnData.buyPrefix) {
				returnData.buyPrefix.charAt(0).toUpperCase() + returnData.buyPrefix.slice(1);
			}else{
				returnData.buyPrefix = "Vraagprijs";
			}

			if(property.ObjectDetails.Koop.KoopConditie == "kosten koper") {
				returnData.buySuffix = "k.k.";
			}else if(property.ObjectDetails.Koop.KoopConditie == "vrij op naam") {
				returnData.buySuffix = "v.o.n.";
			}
		}

		if(returnData.type == "wonen") {
			returnData.buyPrice = property.ObjectDetails.Koop.Koopprijs;
		}else if(returnData.type == "bog") {
			returnData.buyPrice = property.ObjectDetails.Koop.PrijsSpecificatie.Prijs;
		}else{
			returnData.buyPrice = null;
		}
	}else{
		returnData.buy = false;
		returnData.buyPrice = null;
	}

	if(property.ObjectDetails.hasOwnProperty("Huur")) {
		returnData.rent = true;

		returnData.rentPrefix = "Huurprijs";
		switch(property.ObjectDetails.Huur.HuurConditie) {
			case "per jaar": 
				returnData.rentSuffix = "/ j.";
			break;
			case "per vierkante meter per jaar":
				returnData.rentSuffix = "/ m<sup>2</sup> / j.";
			break;
			default:
				returnData.rentSuffix = "/ mnd";
			break;
		}
		
		if(returnData.type == "wonen") {
			returnData.rentPrice = property.ObjectDetails.Huur.Huurprijs;
		}else if(returnData.type == "bog") {
			returnData.rentPrice = property.ObjectDetails.Huur.PrijsSpecificatie.Prijs;
		}else{
			returnData.rentPrice = null;
		}
	}else{
		returnData.rent = false;
		returnData.rentPrice = null;
	}

	returnData.systemId = property.ObjectSystemID;
	returnData.objectCode = property.ObjectCode;
	returnData.lastChanged = property.ObjectDetails.DatumWijziging;
	
	returnData.raw = JSON.stringify(property);

	return returnData;
}

class PostgresPropertyTransport {
	constructor(options, parent) {
		this._options = options;

		if(!this._options.pg && !this._options.db) {
			throw new TypeError("'options.pg' or 'options.db' must be set");
		}

		this._parent = parent;
		this._logger = this._parent.logger;

		if(this._options.db) {
			this.pool = new pg.Pool(this._options.db);
		}else if(this._options.pg) {
			this.pool = pg;
		}
	}
	
	/*
		create(property) is called when a new object is retrieved from the XML 
		update. "property" is a JSON representation of the XML object <Object>

		Implementations should return a Promise
	*/
	create(property) {
		var formatted = parseProperty(property);

		return this.pool.query(escape`
			INSERT INTO
				property (
					type,
					"propertyType",
					street,
					number,
					"numberAddition",
					postcode,
					city,
					country,
					rent,
					buy,
					"rentPrice",
					"buyPrice",
					"systemId",
					"objectCode",
					"lastChanged",
					raw,
					"objectStatus",
					"rentPrefix",
					"buyPrefix",
					"rentSuffix",
					"buySuffix"
				)
			VALUES (
				${formatted.type},
				${formatted.propertyType},
				${formatted.street},
				${formatted.number},
				${formatted.numberAddition},
				${formatted.postcode},
				${formatted.city},
				${formatted.country},
				${formatted.rent},
				${formatted.buy},
				${formatted.rentPrice},
				${formatted.buyPrice},
				${formatted.systemId},
				${formatted.objectCode},
				TO_TIMESTAMP(${formatted.lastChanged}, 'YYYY-MM-DD'),
				${formatted.raw},
				${formatted.objectStatus},
				${formatted.rentPrefix},
				${formatted.buyPrefix},
				${formatted.rentSuffix},
				${formatted.buySuffix}
			)
		`)
		.then((result) => {
			return Promise.resolve();
		})
		.catch((err) => {
			this._logger.log("ERR", "Error creating property '" + formatted.systemId + "'", formatted.systemId, err);
			return Promise.reject(err);
		});
	}

	get(id) {
		return this.pool.query(escape`
			SELECT
				raw
			FROM
				property
			WHERE
				"systemId" = ${id}
		`)
		.then((result) => {
			if(result.rows.length == 0) {
				return Promise.resolve();
			}

			return Promise.resolve(result.rows[0].raw);
		})
		.catch((err) => {
			this._logger.log("ERR", "Error retrieving property '" + id + "'", id, err);
			return Promise.reject(err);
		});
	}

	getIds() {
		return this.pool.query(escape`
			SELECT
				"systemId"
			FROM
				property
		`)
		.then((result) => {
			if(result.rows.length == 0) {
				return Promise.resolve([]);
			}

			return Promise.resolve(result.rows.map((row) => { return row.systemId; }));
		})
		.catch((err) => {
			this._logger.log("ERR", "Error retrieving property IDs", null, err);
			return Promise.reject(err);
		});
	}

	update(id, property) {
		var formatted = parseProperty(property);

		return this.pool.query(escape`
			UPDATE
				property
			SET
				type = ${formatted.type},
				"propertyType" = ${formatted.propertyType},
				street = ${formatted.street},
				number = ${formatted.number},
				"numberAddition" = ${formatted.numberAddition},
				postcode = ${formatted.postcode},
				city = ${formatted.city},
				country = ${formatted.country},
				rent = ${formatted.rent},
				buy = ${formatted.buy},
				"rentPrice" = ${formatted.rentPrice},
				"buyPrice" = ${formatted.buyPrice},
				"objectCode" = ${formatted.objectCode},
				"lastChanged" = TO_TIMESTAMP(${formatted.lastChanged}, 'YYYY-MM-DD'),
				raw = ${formatted.raw},
				"objectStatus" = ${formatted.objectStatus},
				"rentPrefix" = ${formatted.rentPrefix},
				"buyPrefix" = ${formatted.buyPrefix},
				"rentSuffix" = ${formatted.rentSuffix},
				"buySuffix" = ${formatted.buySuffix}
			WHERE
				"systemId" = ${id}
		`)
		.then((result) => {
			return Promise.resolve();
		})
		.catch((err) => {
			this._logger.log("ERR", "Error updating property '" + id + "'", id, err);
			return Promise.reject(err);
		});
	}

	remove(id) {
		return this._parent._mediaUpdater.removeAll(id)
		.then(() => {
			return this.pool.query(escape`
				DELETE FROM
					property
				WHERE
					"systemId" = ${id}
			`);
		})
		.then((result) => {
			return Promise.resolve();
		})
		.catch((err) => {
			this._logger.log("ERR", "Error removing property '" + id + "'", id, err);
			return Promise.reject(err);
		});
	}

	queueRemoval(id, date) {
		return this.pool.query(escape`
			INSERT INTO
				remove_queue (
					"systemId",
					"removalDate"
				)
			VALUES (
				${id},
				TO_TIMESTAMP(${date}, 'YYYY-MM-DD')
			)
		`)
		.then((result) => {
			return Promise.resolve();
		})
		.catch((err) => {
			this._logger.log("ERR", "Error adding to removal queue for date '" + date + "'", id, err);
			return Promise.resolve();
		});
	}

	isQueued(id) {
		return this.pool.query(escape`
			SELECT
				"systemId",
				"removalDate"
			FROM
				remove_queue
			WHERE
				"systemId" = ${id}
		`)
		.then((result) => {
			return Promise.resolve(result.rows.length == 1);
		})
		.catch((err) => {
			this._logger.log("ERR", "Error retrieving removal queue status", id, err);
			return Promise.resolve(false);
		});
	}

	getRemovalQueue() {
		return this.pool.query(escape`
			SELECT
				"systemId",
				"removalDate"
			FROM
				remove_queue
		`)
		.then((result) => {
			return Promise.resolve(result.rows);
		})
		.catch((err) => {
			this._logger.log("ERR", "Error retrieving removal queue", null, err);
			return Promise.resolve([]);
		});
	}

	getReadyForRemoval() {
		return this.pool.query(escape`
			SELECT
				"systemId"
			FROM
				remove_queue
			WHERE
				"removalDate" <= NOW()
		`)
		.then((result) => {
			return Promise.resolve(result.rows.map((row) => { return row.systemId; }));
		})
		.catch((err) => {
			this._logger.log("ERR", "Error retrieving removal queue", null, err);
			return Promise.resolve([]);
		});
	}

	/* MEDIA */
	getMainImage(id) {
		return this.pool.query(escape`
			SELECT
				filename,
				"localName"
			FROM
				image
			WHERE
				"systemId" = ${id}
			ORDER BY
				"displayOrder" ASC
			LIMIT 1
		`)
		.then((result) => {
			return Promise.resolve(result.rows[0]);
		})
		.catch((err) => {
			this._logger.log("ERR", "Error retrieving main image", id, err);
			return Promise.resolve();
		});
	}

	getImages(id) {
		return this.pool.query(escape`
			SELECT
				filename,
				"localName"
			FROM
				image
			WHERE
				"systemId" = ${id}
			ORDER BY
				"displayOrder" ASC
		`)
		.then((result) => {
			return Promise.resolve(result.rows);
		})
		.catch((err) => {
			this._logger.log("ERR", "Error retrieving images", id, err);
			return Promise.resolve([]);
		});
	}

	addImage(id, filename, localName) {
		return this.pool.query(escape`
			INSERT INTO
				image (
					"systemId",
					filename,
					"localName"
				)
			VALUES (
				${id},
				${filename},
				${localName}
			)
		`)
		.then((result) => {
			return Promise.resolve();
		})
		.catch((err) => {
			this._logger.log("ERR", "Error adding image '" + filename + "'", id, err);
			return Promise.resolve();
		});
	}

	updateDisplayOrder(id, filenames) {
		if(!Array.isArray(filenames)) {
			filenames = [filenames];
		}

		if(filenames.length == 0) {
			return Promise.resolve();
		}

		var query = escape`
			UPDATE
				image AS i
			SET
				"displayOrder" = t."displayOrder"
			FROM (
				VALUES
		`;

		var count = -1;
		query.append(filenames.map((filename) => {
			count++;
			return `('${id}', '${filename}', ${count})`;
		}).join(","));
		query.append(escape`
			) AS t(sid, filename, "displayOrder")
			WHERE
				t.sid = i."systemId" AND
				t.filename = i."filename"
		`);
		
		return this.pool.query(query)
		.then(() => {
			return Promise.resolve();
		})
		.catch((err) => {
			this._logger.log("ERR", "Error updating display order", id, err);
			return Promise.resolve();
		});
	}

	removeImage(id, filename) {
		return this.pool.query(escape`
			DELETE FROM
				image
			WHERE
				"systemId" = ${id} AND
				filename = ${filename}
		`)
		.then((result) => {
			return Promise.resolve();
		})
		.catch((err) => {
			this._logger.log("ERR", "Error removing image '" + filename + "'", id, err);
			return Promise.resolve();
		});
	}
}

module.exports = PostgresPropertyTransport;