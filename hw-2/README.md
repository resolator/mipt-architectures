# hw-2
This home work is about addin Apache Ignite caching to the POCO server from hw-1.

Do not forget to run `docker-compose up -d` in `docker` directory to run Apache Ignite cache servers.

## API

- `GET /person?login=some_login` - find and show user with this "some_login" login. This request uses read-through cache policy.
- `GET /person?login=some_login&no_cache` - find and show user with this "some_login" login bypassing cache.
- `GET /person?search&first_name=v&last_name=f` - search users with first name mask "v" and last name mask "f".
- `POST /person?add&first_name=Viktor&last_name=Fet&login=some_login&age=23` - add new user. This request uses write-through cache policy.
