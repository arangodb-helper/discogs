(function() {
    try {
	db._drop("masters2");
    } catch (e) {
    }

    db._create("masters2");

    try {
	db._drop("master_to_artists");
    } catch (e) {
    }

    db._createEdgeCollection("master_to_artists");

    print("loading artists names-to-id map");

    let cursor = db._query("FOR a IN artists2 RETURN { id: a._id, name: a.name }");

    let name2id = {};

    while (cursor.hasNext()) {
	let n = cursor.next();

	name2id[n.name] = n.id;
    }

    print("loading masters oid-to-loc map");

    cursor = db._query("FOR a IN releases2 FILTER a.master_id != 0 RETURN { id: a.master_id, loc: a.location }");

    const countries = [ "US", "US", "US", "US", "GB", "GB", "GB", "JP", "DE", "DE", "FR", "RU" ];
    let m = 0;

    let oid2loc = {};

    while (cursor.hasNext()) {
	let n = cursor.next();
	let l = n.loc;

	if (l === null || l === undefined) {
	    l = countries[m];
	    m = (m + 1) % countries.length;
	}

	oid2loc[n.id] = l;
    }

    print("creating entries in masters2");

    cursor = db._query("FOR m IN masters RETURN m._key");

    let artistsMap = {};

    while (cursor.hasNext()) {
	let n = db.masters.document(cursor.next());
        let artists = n.artists;
	let key = n.id;
	let location = oid2loc[key];

	if (location === null || location === undefined) {
	    location = countries[m];
	    m = (m + 1) % countries.length;
	}

	delete n.artists;

	n.location = location;
	n._key = location + ":" + key;
	n.oid = key;

	let id = db.masters2.save(n)._id;

        for (let k = 0; k < artists.length; ++k) {
	    let artist = artists[k];
	    let aid = name2id[artist];

	    if (aid === "" || aid === undefined) {
		continue;
	    }

	    if (!artistsMap.hasOwnProperty(id)) {
		artistsMap[id] = [];
	    }

	    artistsMap[id].push(aid);
	}
    }

    print("creating master_to_artists relation");

    for (let n in artistsMap) {
	if (artistsMap.hasOwnProperty(n)) {
	    let artists = artistsMap[n];

	    for (let k = 0; k < artists.length; ++k) {
		let t = artists[k];

		db.master_to_artists.save({ "_from": n, "_to": t });
	    }
	}
    }
})
