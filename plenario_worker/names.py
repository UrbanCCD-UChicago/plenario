import random


def generate_name():
    # Generate a (highly probably) unique name.
    # References so far:
    # - The Hitchhiker's Guide to the Galaxy
    # - Rick and Morty

    firstnames = [
        "7dimensional", "abhorrent", "aldebaran", "algorithmic", "alphacentauri", "altair", "amusing", "antares",
        "arcturus", "argabuthon", "arkintoofle", "artificial", "asgardian", "barnardian", "bartledan", "bartraxian",
        "betelgeuse", "bethselamin", "birdy", "bistro", "blagulon", "blippy", "brantisvogan", "brazen", "brequinda",
        "brontitall", "broopy", "burger", "chitzy", "cinematic", "citadel", "cromulon", "cronenberg", "damogran",
        "dangrabad", "dimension", "doopidoo", "dordellis", "earthly", "evil", "fair", "fallia", "fancy", "fast", "fat",
        "flanux", "flargathon", "flying", "folfanga", "foth", "fragrant", "frastra", "frazfraga", "freaky", "free",
        "frigid", "frogstar", "frosty", "funny", "furious", "furry", "fuzzy", "gagrakacka", "galactic", "gazorpazorp",
        "geared", "george", "giant", "golgafrincham", "gooey", "greasy", "hamster", "hanwavel", "hastromil", "hawalius",
        "hollop", "holographic", "hopeful", "horsehead", "hunian", "huntringfurl", "illustrious", "imperial",
        "indignant", "indirect", "insipid", "interdimensional", "ionic", "ironic", "jackson", "jaglan", "jajazikstak",
        "jerryboree", "jesse", "kakrafoon", "kif", "king", "krikkit", "lamuella", "lilbits", "magrathea", "maximegalon",
        "megabrantis", "megadodo", "millenium", "milliways", "nano", "nibblonian", "nowwhat", "oglaroon", "omicronian",
        "orionbeta", "paranoid", "party", "paul", "planets", "pleiades", "poghril", "purging", "qualactin", "qvarne",
        "rambo", "rare", "ratty", "reluctant", "respectful", "ridiculous", "river", "rogue", "rude", "santraginus",
        "saquopilia", "sarcastic", "shoed", "silly", "siriustau", "soft", "solar", "spherical", "sqornshellous",
        "stavromula", "stegbartle", "stellar", "striterax", "stug", "super", "terrace", "tinny", "traal", "trusty",
        "uber", "universal", "ursaminor", "viltvodle", "violent", "vod", "vogsphere", "voondon", "will", "wubaluba",
        "xaxis", "ysllodins", "zapp", "zarss", "zentalquabula", "zirzla"
    ]

    lastnames = [
        "abra", "aerodactyl", "agda", "agrajag", "alakazam", "alexander", "allitnils", "amy", "android", "anjie",
        "annie", "arbok", "arcanine", "arkleseizure", "armagheadon", "arthricia", "articuno", "aseed", "bangbang",
        "barmen", "bartlett", "beckman", "beeblebrox", "beedrill", "bellsprout", "bender", "beta-seven", "bird",
        "birdperson", "blastoise", "blimblam", "blueshirt", "bob", "bodyguard", "bozo", "bracho", "brannigan",
        "bulbasaur", "butterfree", "calculon", "captain", "carhart", "carlinton", "caterpie", "cave", "caveman",
        "centaur", "chansey", "charizard", "charmander", "charmeleon", "clefable", "clefairy", "clone", "cloyster",
        "colin", "conrad", "consultant", "creature", "cromulons", "cronenberg", "crushinator", "cubone", "cyborg",
        "cynthia", "dale", "deepthought", "dent", "desiato", "dewgong", "diglett", "disasterarea", "dish", "ditto",
        "dodrio", "doduo", "doofus", "dragonair", "dragonite", "dratini", "drowzee", "dubdub", "dugtrio", "eccentrica",
        "eddie", "eevee", "effrafax", "ekans", "electabuzz", "electrode", "elvis", "elzar", "emperor", "engler",
        "exeggcute", "exeggutor", "falcon", "farfetchd", "farnsworth", "fearow", "fenchurch", "fitzmelton", "flareon",
        "flexo", "frankie", "frat", "frootmig", "fry", "gag", "gail", "garblovians", "gargravarr", "garkbit",
        "garmanarnar", "gastly", "gazorpazorpfield", "gengar", "genghis", "geodude", "glipglop", "gloom", "god",
        "gogrilla", "golbat", "goldeen", "golduck", "golem", "golgafrinchans", "googleplex", "graduate", "graveler",
        "grimer", "growlithe", "grunthos", "guenter", "gyarados", "hactar", "haggunenon", "hairdresser", "haunter",
        "head", "heimdall", "heisenbug", "hermes", "hitmonchan", "hitmonlee", "hollop", "hooli", "horsea", "hunter",
        "hurtenflurst", "hypno", "ivysaur", "ix", "jerry", "jigglypuff", "johnson", "jolteon", "jynx", "kabuto",
        "kabutops", "kadabra", "kakuna", "kangaskhan", "kapelsen", "karl", "kavula", "kingler", "koffing", "krabby",
        "krikkit", "krikkiters", "kroker", "kwaltz", "lallafa", "lapras", "leela", "lickitung", "lintilla", "loonquawl",
        "lord", "lunkwill", "lury", "lyricon", "machamp", "machoke", "machop", "magician", "magikarp", "magmar",
        "magnemite", "magneton", "majikthise", "mankey", "mark", "marketer", "marowak", "marvin", "mckenna", "meeseeks",
        "megadodo", "megafreighter", "meme", "meowth", "metapod", "mew", "mewtwo", "minetti", "moe", "moltres", "mom",
        "morbo", "morty", "mrmime", "muk", "murray", "nibbler", "nidoking", "nidoqueen", "nidoran", "nidorina",
        "nidorino", "ninetales", "nullify", "numberone", "numbertwo", "oddish", "officials", "omanyte", "omastar",
        "omnicognate", "onix", "oolon", "pag", "paras", "parasect", "persian", "phouchg", "pidgeot", "pidgeotto",
        "pidgey", "pikachu", "pinsir", "plumbus", "poet", "poles", "poliwag", "poliwhirl", "poliwrath", "ponyta",
        "poodoo", "porygon", "prak", "pralite", "president", "primeape", "princess", "prosser", "psyduck", "questular",
        "quordlepleen", "raffle", "raichu", "rapidash", "raticate", "rattata", "receptionists", "rhydon", "rhyhorn",
        "rick", "robot", "roosta", "ruler", "russell", "sanchez", "sandshrew", "sandslash", "sanitizer", "saunders",
        "scary", "scruffy", "scyther", "seadra", "seaking", "seel", "sheila", "shellder", "slartibartfast", "slowbro",
        "slowpoke", "smith", "smithers", "snorlax", "snuffles", "spearow", "squirtle", "starmie", "staryu", "strinder",
        "sulijoo", "tangela", "tauros", "tentacool", "tentacruel", "thor", "thrashbarg", "tim", "traflorkians",
        "tribesmen", "trintragula", "vantrashell", "vaporeon", "varntvar", "venomoth", "venonat", "venusaur",
        "versenwald", "victreebel", "vileplume", "vogon", "voltorb", "voojagig", "vranx", "vroomfondel", "vulpix",
        "wartortle", "weedle", "weepinbell", "weezing", "werdle", "whale", "wigglytuff", "wong", "wonko", "wowbagger",
        "wsogmm", "zapdos", "zaphod", "zarniwoop", "zarquon", "zem", "zoidberg", "zubat"
    ]

    return random.choice(firstnames) + "_" + random.choice(lastnames)
