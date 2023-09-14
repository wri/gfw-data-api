GOOD_ORGANIZATIONS = ["WRI", "Global Forest Watch"]
GOOD_EMAILS = [
    "info@wri.org",
    "admin@globalforestwatch.org",
    "firstname.lastname@test.com",
]
GOOD_DOMAINS = [
    "www.globalforestwatch.org",
    "*.globalforestwatch.org",
    "globalforestwatch.org",
    "localhost",
]

BAD_EMAILS = ["not an email", "also_not@n-email", "nope", None]
BAD_DOMAINS = [
    "www.*.com",
    "*",
    "www.test*.org",
    "www.test.*",
    "*.com",
    "globalforestwatch.org:443",
    "localhost:3000",
]
