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

    print("creating entries in masters2");

    cursor = db._query("FOR m IN masters RETURN m");

    let artistsMap = {};

    while (cursor.hasNext()) {
	let n = cursor.next();
        let artists = n.artists;

	delete n.artists;

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
