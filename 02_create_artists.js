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

    print("creating entries in artists2");

    let cursor = db._query("FOR a IN artists RETURN a");

    let groupMap = {};
    let names = {};

    while (cursor.hasNext()) {
	let n = cursor.next();
        let groups = n.groups;

	delete n.groups;
	delete n.members;

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
