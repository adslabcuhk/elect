import sys

if len(sys.argv) != 2:
    print("Usage: python genToken.py num_nodes")
    sys.exit(1)
num_nodes = int(sys.argv[1])
num_tokens = 1

print(
    "\n".join(
        [
            "[Node {}]\ninitial_token: {}".format(
                r + 1,
                ",".join(
                    [
                        str(
                            round(
                                (2**64 / (num_tokens * num_nodes))
                                * (t * num_nodes + r)
                            )
                            - 2**63
                        )
                        for t in range(num_tokens)
                    ]
                ),
            )
            for r in range(num_nodes)
        ]
    )
)
