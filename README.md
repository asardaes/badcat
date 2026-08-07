Prowlarr's Full Sync overwrites any custom categories you may have configured for indexers inside specific apps.
This program enables you to maintain custom indexer categories per app persisted independently of Prowlarr,
resyncing whenever a mismatch is detected.

The input to `badcat` is a JSON specified in an environment variable `INSTANCE_CONFIG_JSON`,
which should have content like:

```json
{
    "arr": {
        "url": "http://arr:8989",
        "api_key": "foo"
    },
    "brr": {
        "url": "http://brr:7878",
        "api_key": "barr"
    }
}
```

You can add as many entries as you want.

You should also specify an output folder in an environment variable `OUTPUT_FOLDER`.
During the first run, that folder will be populated with one subfolder per app,
and each subfolder will have one JSON file per indexer that looks like this:

```json
{
  "raw_name": "Nyaa.si (Prowlarr)",
  "desired_categories": {
    "categories": [
      5000
    ],
    "animeCategories": [
      5070
    ]
  },
  "available_categories": [ "..." ]
}
```

In those files you can enter the categories you want under `desired_categories`.
`badcat` will cross-check the categories in those files against the configuration in the respective app:

- during startup
- after you edit and save a JSON file
- if it detects Prowlarr updated the indexers

Only categories are monitored, nothing else gets modified.

A Docker compose file could look like this:

```yaml
services:
  badcat:
    container_name: badcat
    image: ghcr.io/asardaes/badcat:main
    restart: unless-stopped
    user: ${USER_ID}:${GROUP_ID}
    environment:
      - INSTANCE_CONFIG_JSON=/config/instances.json
      - OUTPUT_FOLDER=/config/cats
    volumes:
      - ./badcat:/config
```
