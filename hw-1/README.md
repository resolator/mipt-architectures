# hw-1
This home work is about POCO server api and database interaction.

Use `git lfs pull` to download the `./fill-db/data.json` file.

## API

- `GET /person?login=some_login` - find and show user with this "some_login" login
- `GET /person?search&first_name=v&last_name=f` - search users with first name mask "v" and last name mask "f"
- `POST /person?add&first_name=Viktor&last_name=Fet&login=some_login&age=23` - add new user

## Directories structure

- `fill-db` - contains C++17 project to fill `Person` database
- `server` - contains C++17 project with POCO server to interact with MySQL
