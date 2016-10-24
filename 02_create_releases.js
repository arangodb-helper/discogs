(function() {
    try {
	db._drop("releases2");
    } catch (e) {
    }

    db._create("releases2");

    try {
	db._drop("label_to_releases");
    } catch (e) {
    }

    db._createEdgeCollection("label_to_releases");

    print("loading labels name-to-id map");

    cursor = db._query("FOR l IN labels2 RETURN { id: l._id, name: l.name, location: l.location }");

    let label2id = {};
    let label2loc = {};

    while (cursor.hasNext()) {
	let n = cursor.next();

	label2id[n.name] = n.id;
	label2loc[n.name] = n.location;
    }

    print("creating entries in releases2");

    cursor = db._query("FOR r IN releases RETURN r._key");

    let labelsMap = {};

    while (cursor.hasNext()) {
	let n = db.releases.document(cursor.next());
        let title = n.title;
	let labels = n.labels;
	let location = "US";
        let key = n.id;

	if (labels.length > 0) {
	    location = label2loc[labels[0].name] || "US";
	}

	delete n.id;
	delete n.labels;

	n.location = location;
	n._key = n.location + ":" + key;
	n.oid = key;

	let id = db.releases2.save(n)._id;

        for (let k = 0; k < labels.length; ++k) {
	    let label = labels[k].name;
	    let aid = label2id[label];

	    if (aid === "" || aid === undefined) {
		continue;
	    }

	    if (!labelsMap.hasOwnProperty(id)) {
		labelsMap[id] = [];
	    }

	    labelsMap[id].push(aid);
	}
    }

    db.releases2.ensureIndex({ type: "hash", fields: ["oid"] })

    print("creating label_to_releases relation");

    for (let n in labelsMap) {
	if (labelsMap.hasOwnProperty(n)) {
	    let rels = labelsMap[n];

	    for (let k = 0; k < rels.length; ++k) {
		let t = rels[k];

		db.label_to_releases.save({ "_from": t, "_to": n });
	    }
	}
    }
})
