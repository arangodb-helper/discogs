const fs = require("fs");

const colls = [ "labels2", "artists2", "masters2", "releases2", "group_members", "label_to_releases",
                "master_to_artists", "master_to_releases", "parent_label" ];


for (let i = 0; i < colls.length; ++i) {
  const collection = colls[i];

  print("exporting " + collection);

  const filename =  collection + ".json";
    
  fs.write(filename, "");

  let data = JSON.parse(arango.POST_RAW("/_api/export?collection=" + encodeURIComponent(collection), "{}").body);

  while (true) {
    var res = "";
    data.result.forEach(function(r) {
	res += JSON.stringify(r) + "\n";
    });
      
    fs.append(filename, res);
      
    if (!data.hasMore) {
	break;
    }

    data = JSON.parse(arango.PUT_RAW("/_api/export/" + encodeURIComponent(data.id), "").body);
  }
}
