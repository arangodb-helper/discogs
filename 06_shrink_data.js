(function() {
    try {
	db._drop("releases3");
    } catch (e) {
    }

    db._create("releases3");

    print("shrinking releases entries in releases3");

    cursor = db._query("FOR r IN releases2 RETURN r._key");

    while (cursor.hasNext()) {
	let n = db.releases2.document(cursor.next());
	let tracklist = n.tracklist;

	delete n._id;
	delete n._rev;
	delete n.barcode;
	delete n.artistJoins;
	delete n.extraartists;
	delete n.images;
	delete n.formats;
	delete n.tracklist;

	for (let i = 0; i < tracklist.length; ++i) {
	    tracklist[i] = tracklist[i].title;
	}

	n.tracklist = tracklist;

	db.releases3.save(n)._id;
    }
})
