[![progress-banner](https://backend.codecrafters.io/progress/redis/4453e343-a018-4d87-a9e3-472b62a44aa9)](https://app.codecrafters.io/users/sherubthakur?r=2qF)

This is a starting point for Haskell solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

In this challenge, you'll build a toy Redis clone that's capable of handling
basic commands like `PING`, `SET` and `GET`. Along the way we'll learn about
event loops, the Redis protocol and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Passing the first stage

The entry point for your Redis implementation is in `app/Main.hs`. Study and
uncomment the relevant code, and push your changes to pass the first stage:

```sh
git add .
git commit -m "pass 1st stage" # any msg
git push origin master
```

That's all!

# Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `stack` installed locally
1. Run `./spawn_redis_server.sh` to run your Redis server, which is implemented
   in `app/Main.hs`.
1. Commit your changes and run `git push origin master` to submit your solution
   to CodeCrafters. Test output will be streamed to your terminal.
