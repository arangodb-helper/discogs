(function() {
    const countries = [ "US", "US", "US", "US", "GB", "GB", "GB", "JP", "DE", "DE", "FR", "RU" ];

    print("finding location for roots");

    let cursor = db._query("FOR l IN labels FILTER l.parentLabel == '' RETURN l");
    let m = 0;

    let map = {};

    while (cursor.hasNext()) {
	let n = cursor.next();
	let c = countries[m];

	m = (m + 1) % countries.length;

	map[n.name] = c;
    }

    print("finding location for children");

    cursor = db._query("FOR l IN labels FILTER l.parentLabel != '' RETURN l");

    while (cursor.hasNext()) {
	let n = cursor.next();
	let p = map[n.parentLabel];
	let c = "US";

	if (p !== "" && p !== undefined) {
	    c = p;
	} else {
	    c = countries[m];
	    m = (m + 1) % countries.length;
	}

	map[n.name] = c;
    }

    try {
	db._drop("labels2");
    } catch (e) {
    }

    db._create("labels2");

    try {
	db._drop("parent_label");
    } catch (e) {
    }

    db._createEdgeCollection("parent_label");

    cursor = db._query("FOR l IN labels RETURN l");

    print("creating entries in labels2");

    let parents = {};
    let names = {};

    while (cursor.hasNext()) {
	let n = cursor.next();

	if (n.parentLabel !== "" && n.parentLabel !== undefined) {
	    parents[n.name] = n.parentLabel;
	}

	delete n.parentLabel;
	delete n.sublabels;

	n.location = map[n.name] || "US";

	let id = db.labels2.save(n);

	names[n.name] = id._id;
    }

    print("creating parent relation");

    for (let n in parents) {
	if (parents.hasOwnProperty(n)) {
	    let f = names[n];
	    let t = names[parents[n]];

	    if (f !== "" && f !== undefined && t !== "" && t !== undefined) {
		db.parent_label.save({ "_from": f, "_to": t });
	    }
	}
    }
})
