db.createUser(
    {
        user: "skald",
        pwd: "skald",
        roles: [
            {
                role: "readWrite",
                db: "skald"
            }
        ]
    }
);