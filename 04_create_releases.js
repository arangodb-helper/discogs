(function() {
    try {
	db._drop("releases2");
    } catch (e) {
    }

    db._create("releases2");

    try {
	db._drop("master_to_releases");
    } catch (e) {
    }

    db._createEdgeCollection("master_to_releases");

    try {
	db._drop("label_to_releases");
    } catch (e) {
    }

    db._createEdgeCollection("label_to_releases");

    print("loading masters title-to-id map");

    let cursor = db._query("FOR m IN masters2 RETURN { id: m._id, name: m.title }");
    let master2id = {};

    while (cursor.hasNext()) {
	let n = cursor.next();

	master2id[n.name] = n.id;
    }

    print("loading labels name-to-id map");

    cursor = db._query("FOR l IN labels2 RETURN { id: l._id, name: l.name }");
    let label2id = {};

    while (cursor.hasNext()) {
	let n = cursor.next();

	label2id[n.name] = n.id;
    }

    print("creating entries in releases2");

    cursor = db._query("FOR r IN releases RETURN r");

    let labelsMap = {};
    let mastersMap = {};

    while (cursor.hasNext()) {
	let n = cursor.next();
        let title = n.title;
	let labels = n.labels;

	delete n.labels;

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

	let mid = master2id[title];

	if (mid !== "" && mid !== undefined) {
	    if (!mastersMap.hasOwnProperty(mid)) {
		mastersMap[mid] = [];
	    }

	    mastersMap[mid].push(id);
	}
    }

    print("creating master_to_releases relation");

    for (let n in mastersMap) {
	if (mastersMap.hasOwnProperty(n)) {
	    let rels = mastersMap[n];

	    for (let k = 0; k < rels.length; ++k) {
		let t = rels[k];

		db.master_to_releases.save({ "_from": n, "_to": t });
	    }
	}
    }

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
