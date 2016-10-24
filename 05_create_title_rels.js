(function() {
    try {
	db._drop("master_to_releases");
    } catch (e) {
    }

    db._createEdgeCollection("master_to_releases");

    print("loading master titles");

    let cursor = db._query("FOR a IN masters2 RETURN { id: a._id, name: a.title }");

    let title2id = {};

    while (cursor.hasNext()) {
	let n = cursor.next();

	title2id[n.name] = n.id;
    }

    print("creating entries in master_to_releases");

    cursor = db._query("FOR m IN releases2 RETURN { id: m._id, title: m.title }");

    while (cursor.hasNext()) {
	let n = cursor.next();
	let mid = title2id[n.title];

	if (mid !== null && mid !== undefined) {
          db.master_to_releases.save({ "_from": n.id, "_to": mid });
	}
    }
})
