from pybit.unified_trading import HTTP
# session = HTTP(
#     testnet=False,
#     api_key="yEww0avtjsoYtnL49e",
#     api_secret="70u8KNxdTj3F68xZ9SvcygnYWulfqnRRxDw9",
# )
# session = HTTP(
#     testnet=False,
#     api_key="KgQ6PbW4XQ6XHc29qV",
#     api_secret="cTqUGDxoncZ7eaDVWEb1Xr6SRzAhDKGoEhFc",
# )

session = HTTP(
    testnet=False,
    api_key="1Gluy6GE6BfIveSBOZ",
    api_secret="2pinNuvPz6QaPTgkzZKzD82YXQm222N731yW",
)
print(session.get_api_key_information())

