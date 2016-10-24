(function() {
    try {
	db._drop("artists2");
    } catch (e) {
    }

    db._create("artists2");

    try {
	db._drop("group_members");
    } catch (e) {
    }

    db._createEdgeCollection("group_members");

    print("guessing locations");

    let id2loc = {};

    let cursor = db._query("FOR a IN artists LET loc = (FOR m IN masters FILTER a.name in m.artists and m.main_release != 0 FOR r in releases2 FILTER to_number(m.main_release) == r.oid COLLECT loc = r.location WITH COUNT INTO count SORT count DESC LIMIT 1 RETURN loc) RETURN { id: a.id, loc: loc[0] }");

    const countries = [ "US", "US", "US", "US", "GB", "GB", "GB", "JP", "DE", "DE", "FR", "RU" ];
    let m = 0;

    while (cursor.hasNext()) {
	let n = cursor.next();
	let l = n.loc;

	if (l === null || l === undefined) {
	    l = countries[m];
	    m = (m + 1) % countries.length;
	}

	id2loc[n.id] = l;
    }

    print("creating entries in artists2");

    cursor = db._query("FOR a IN artists RETURN a._key");

    let groupMap = {};
    let names = {};

    while (cursor.hasNext()) {
	let n = db.artists.document(cursor.next());
        let groups = n.groups;
        let key = n.id;
        let location = id2loc[key];

	delete n.groups;
	delete n.members;

	n.location = location;
	n._key = n.location + ":" + key;
	n.oid = key;

	let id = db.artists2.save(n);
	names[n.name] = id._id;

        for (let k = 0; k < groups.length; ++k) {
	    let group = groups[k];

	    if (!groupMap.hasOwnProperty(group)) {
		groupMap[group] = [];
	    }

	    groupMap[group].push(id._id);
	}
    }

    print("creating members relation");

    for (let n in groupMap) {
	if (groupMap.hasOwnProperty(n)) {
	    let groups = groupMap[n];
	    let f = names[n];

	    if (f !== "" && f !== undefined) {
		for (let k = 0; k < groups.length; ++k) {
		    let t = groups[k];

		    db.group_members.save({ "_from": f, "_to": t });
		}
	    }
	}
    }
})
