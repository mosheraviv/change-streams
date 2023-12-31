// generated with the following bash
// #!/bin/bash
// LEN=$(( $(wc -l < /usr/share/dict/words) + 1))
// for i in {1..1000}; do
//    head -$((${RANDOM} % ${LEN} )) /usr/share/dict/words | tail -1
// done | sed -e '1 i\const words=[' -e 's/^\(.*\)$/   "\1",/'
const WORDS=[
    "chimney's",
    "consummated",
    "cosmos",
    "Seattle's",
    "charms",
    "Lyndon",
    "archivists",
    "communication",
    "Slashdot's",
    "Baldwin's",
    "Paul",
    "bah",
    "Reilly's",
    "alphabet",
    "Napoleonic",
    "Irisher",
    "baseboard",
    "Costner",
    "connector",
    "ambushed",
    "accidental's",
    "Menelik's",
    "conqueror",
    "commotion's",
    "amanuensis",
    "Cid",
    "blindingly",
    "basked",
    "brandied",
    "campuses",
    "audibles",
    "assent's",
    "brusqueness's",
    "asterisk's",
    "Augustine",
    "asylum",
    "clunkiest",
    "aviators",
    "alights",
    "Nationwide's",
    "bedpan",
    "cohort's",
    "corded",
    "Brenton's",
    "Vicente's",
    "Milken's",
    "Malaysians",
    "Emacs",
    "Shenandoah",
    "Katy's",
    "blackballing",
    "complexity's",
    "aftershock",
    "baser",
    "chicer",
    "Cotopaxi's",
    "brontosauruses",
    "antagonist's",
    "bunched",
    "achieve",
    "Rockne's",
    "Senates",
    "Elisha",
    "Foley",
    "Balder",
    "Gillette",
    "butte",
    "conclave's",
    "cardiograms",
    "Oranjestad's",
    "Lopez",
    "Zeno",
    "Learjet's",
    "LPN's",
    "ambiguity",
    "bomber",
    "alters",
    "chancel",
    "ciphering",
    "corkscrewing",
    "Chauncey's",
    "apologias",
    "colloquy's",
    "author's",
    "Lepidus",
    "burgher's",
    "boundaries",
    "Aspidiske",
    "Polyhymnia",
    "addenda",
    "airmailed",
    "brewing",
    "conciliate",
    "bespeaks",
    "combine",
    "corduroys's",
    "chokers",
    "biweekly",
    "commensurable",
    "benefactions",
    "Boer",
    "Heinz",
    "Hamlin's",
    "Mormon's",
    "Theiler's",
    "breed's",
    "Billy",
    "Amado's",
    "armful's",
    "comported",
    "course's",
    "bigamy's",
    "adiós",
    "collies",
    "Novgorod",
    "cabbing",
    "biggie's",
    "Catholics",
    "cartel's",
    "Buddha",
    "arthritis's",
    "Qom's",
    "catacomb's",
    "align",
    "Burgundy",
    "branch",
    "aerating",
    "conditioner",
    "Tuscon's",
    "Twitter",
    "authorizing",
    "acrobatics",
    "barbecuing",
    "castoffs",
    "centered",
    "D's",
    "antiknock",
    "cloning",
    "Donnell",
    "Hellespont",
    "catholicity",
    "Dominican",
    "cloudiest",
    "Plasticine",
    "Belshazzar's",
    "Katherine",
    "Filipino's",
    "bragged",
    "Namath",
    "corolla's",
    "carom",
    "concedes",
    "Phobos",
    "baggiest",
    "Yossarian",
    "Stu",
    "Sargasso",
    "B",
    "Rasmussen's",
    "chlorofluorocarbon",
    "abandoning",
    "Angel's",
    "Rotterdam's",
    "Frye's",
    "challenges",
    "Kate",
    "Maine",
    "Sunbelt",
    "Ebonics",
    "Sennett's",
    "authoritativeness's",
    "converging",
    "agendas",
    "advertised",
    "O'Connell",
    "apparition's",
    "bullfighter's",
    "Zelma",
    "Orbison's",
    "Conakry's",
    "Wests",
    "Gaiman's",
    "Bauhaus",
    "chairmanship's",
    "carouse",
    "affluence",
    "Malinda",
    "Jess's",
    "animating",
    "barrage",
    "Clotho",
    "Carrier's",
    "beige",
    "Yorkshire's",
    "Kristen",
    "adventured",
    "bonfire's",
    "adored",
    "conveyor's",
    "chronicler",
    "confine's",
    "arrowroot",
    "chamomiles",
    "Yekaterinburg",
    "Ethiopia",
    "Haleakala",
    "Chateaubriand",
    "communing",
    "benching",
    "Sundanese",
    "August",
    "Gienah",
    "argot's",
    "Candy's",
    "Arafat",
    "Spengler's",
    "assessor",
    "Mindoro's",
    "bangle",
    "Hilda",
    "coolers",
    "brief's",
    "Mandrell",
    "barge's",
    "Lauri's",
    "adverted",
    "Hilbert",
    "counterfeiting",
    "Burgess",
    "Giza's",
    "choice",
    "Antipas's",
    "buzzard's",
    "blonder",
    "accumulated",
    "concluded",
    "coin",
    "arms",
    "chomped",
    "archbishopric",
    "Bender",
    "cockier",
    "bunks",
    "balmier",
    "correctly",
    "candidness",
    "Ceres's",
    "armature",
    "Danubian",
    "bowlder",
    "canyon",
    "Christmas's",
    "abscissas",
    "Claude's",
    "brightens",
    "browbeating",
    "authorizes",
    "consumption's",
    "broadloom",
    "bistro",
    "Elroy's",
    "auto",
    "boggy",
    "complications",
    "Persepolis's",
    "Kuwait",
    "cicadas",
    "Lucia's",
    "chestnut",
    "brainwashing",
    "cilia",
    "coiffing",
    "bang's",
    "banshee",
    "amphetamine's",
    "backaches",
    "Utopian's",
    "bandstands",
    "Lardner",
    "antihistamines",
    "Sc's",
    "Brunelleschi",
    "beginner",
    "colloquialisms",
    "Gael's",
    "Tauruses",
    "belfries",
    "canasta's",
    "Segundo's",
    "Thorpe",
    "accomplish",
    "alining",
    "Chartism's",
    "Krishna's",
    "XEmacs's",
    "Theosophy",
    "abbess",
    "Procrustes",
    "bluegrass's",
    "Pole",
    "Derrida's",
    "ardor's",
    "Mafia's",
    "burping",
    "Livingston",
    "chaotic",
    "Stanley",
    "bookmaker's",
    "abundance",
    "bellyful",
    "Nate's",
    "column's",
    "briar",
    "Alphecca's",
    "cedes",
    "commotions",
    "Trojans",
    "addictive",
    "bombast's",
    "chows",
    "coquettish",
    "Kwanzaa's",
    "Stanford",
    "Burnside",
    "abbreviations",
    "comments",
    "actuality's",
    "Albany",
    "McGovern's",
    "Kuznets's",
    "Pyrexes",
    "Slackware",
    "checkups",
    "bandages",
    "Hunter's",
    "Dodoma",
    "abstention's",
    "awl's",
    "chiropractic",
    "Aquafresh's",
    "Nicole's",
    "Helene",
    "condolence's",
    "Chianti's",
    "I",
    "burying",
    "Antone",
    "batting",
    "Moiseyev",
    "breezily",
    "agriculture",
    "Maxwell",
    "Alleghenies's",
    "cordoned",
    "Pottawatomie's",
    "accomplice",
    "coddled",
    "clinics",
    "actuated",
    "Houston's",
    "Esau",
    "clustered",
    "audiences",
    "Alembert's",
    "alpha's",
    "centrist",
    "Edwardian's",
    "Byelorussia",
    "Wilmer's",
    "Aleppo's",
    "Clarendon",
    "carpals",
    "Hotpoint's",
    "Bayer",
    "collectivizes",
    "Shi'ite",
    "catamaran's",
    "conceivable",
    "Chimera's",
    "Epicurus",
    "bicameral",
    "axial",
    "Taklamakan's",
    "blackberry",
    "astrophysicists",
    "capriciousness's",
    "Jill",
    "adults",
    "bitters's",
    "breadwinner",
    "botch's",
    "cheerfullest",
    "bandwidth",
    "RAM's",
    "chlorination",
    "Jacob",
    "Janell",
    "Alison",
    "Mauritania",
    "blackhead",
    "Antichrist's",
    "Rostov",
    "advance's",
    "Jody",
    "Sweeney",
    "Thurmond",
    "badge",
    "Louise's",
    "Roseann",
    "bound",
    "Shevardnadze's",
    "Chimera",
    "Rodrick's",
    "chicken",
    "candidacy's",
    "Anatolian's",
    "becalms",
    "BS's",
    "alcove's",
    "Rodriquez's",
    "cabinetmaker's",
    "McLaughlin's",
    "Kuznets's",
    "Thursday's",
    "beautify",
    "Denise",
    "beacon's",
    "coaching",
    "Dürer",
    "conundrums",
    "ascribed",
    "Filipino's",
    "Polynesian's",
    "Everette's",
    "Roderick's",
    "canoeists",
    "argot",
    "Baroda",
    "Carr",
    "Adam",
    "buttermilk's",
    "abbeys",
    "Cid",
    "Goolagong",
    "Pontianak",
    "coat",
    "Herrera's",
    "Bender",
    "conclave",
    "breaching",
    "Swift",
    "bongoes",
    "Bk",
    "besets",
    "Hamburg's",
    "bombed",
    "career's",
    "chiming",
    "availing",
    "connoisseur's",
    "cell's",
    "Larsen",
    "Dahomey",
    "Hume",
    "Scotches",
    "abominations",
    "Anglia",
    "Andrea",
    "airfield's",
    "Comoros",
    "Anselm",
    "Oder's",
    "aridity's",
    "Darwinian",
    "acrobat's",
    "cooler",
    "Arabians",
    "blotching",
    "Aristotle's",
    "continuation's",
    "Walker's",
    "Urban's",
    "biochemical",
    "adverb",
    "Frances",
    "clocking",
    "Louisianian",
    "corks",
    "broil",
    "coiffure's",
    "concatenated",
    "ares",
    "Gandhi's",
    "Blackstone",
    "carcinogenic's",
    "anniversary's",
    "amalgam's",
    "caveman",
    "approximation's",
    "Dachau's",
    "conclave",
    "assistant's",
    "Tonto",
    "airless",
    "boring",
    "cords",
    "adiabatic",
    "browbeating",
    "biplane",
    "all's",
    "bonnie",
    "Gwalior",
    "Argos",
    "Taiyuan",
    "blue's",
    "Haleakala",
    "Odom's",
    "AOL",
    "Genghis",
    "Corning",
    "coin",
    "Cunard's",
    "blast",
    "Casio's",
    "Ganges",
    "adoring",
    "baronial",
    "bombardments",
    "Proust's",
    "Lippmann",
    "changes",
    "Monsanto",
    "cleared",
    "Min",
    "chinos",
    "aureole's",
    "Martha",
    "Rusty's",
    "Sang's",
    "Abernathy",
    "conquers",
    "Ellesmere's",
    "Hagiographa",
    "cornflakes's",
    "antic's",
    "collections",
    "Geritol",
    "communiques",
    "collectivized",
    "Ouagadougou",
    "Backus's",
    "catalogue",
    "Tunguska",
    "brawny",
    "collate",
    "augment",
    "Mari's",
    "anode's",
    "PowerPoint",
    "barbeque's",
    "changeover",
    "Jaime",
    "copulate",
    "bosh",
    "admonish",
    "chameleon's",
    "Ralph's",
    "caseloads",
    "Kojak's",
    "burred",
    "casserole's",
    "Homer's",
    "communally",
    "Anatolian's",
    "Hallmark",
    "acclimatizing",
    "complicate",
    "Plexiglas's",
    "Taoism's",
    "chaparrals",
    "blasphemers",
    "Westminster",
    "before",
    "Goethe's",
    "Kaufman",
    "appropriation's",
    "capsuling",
    "adjudication's",
    "Greensleeves",
    "Guadalajara's",
    "Hatsheput",
    "Axum",
    "Schmidt",
    "Giauque",
    "bundles",
    "conclave",
    "Ares",
    "Flo",
    "bruises",
    "Lully",
    "cone's",
    "apportioning",
    "Watusi",
    "chrysanthemum",
    "asexual",
    "battlements",
    "Romulus",
    "airways",
    "Bangladeshi",
    "ceasing",
    "Sargasso's",
    "breastbones",
    "Nadia",
    "Charmin",
    "comprehensives",
    "ambient",
    "benefit",
    "brandished",
    "Cid",
    "chubbiest",
    "comradeship's",
    "Marshall",
    "cohorts",
    "Othello's",
    "Usenet",
    "beseeches",
    "Marrakesh",
    "Yerevan",
    "charring",
    "Boötes",
    "bucklers",
    "camaraderie",
    "bannister",
    "Langerhans",
    "beckoned",
    "Mable's",
    "compacter",
    "Burton",
    "catbird",
    "contrivance",
    "coiling",
    "Karloff's",
    "blusters",
    "Kasai",
    "Neil's",
    "barrelling",
    "campy",
    "Antigua",
    "Shirley",
    "coachman",
    "closet's",
    "Perth's",
    "Hecate",
    "capacities",
    "bistro",
    "baroness",
    "abolishing",
    "Adelaide",
    "Fox's",
    "Piaget's",
    "Brezhnev",
    "comport",
    "Fry",
    "blundered",
    "Haney",
    "chaff's",
    "Akron's",
    "Delius",
    "columned",
    "ceilings",
    "Chiquita",
    "catheters",
    "Cantabrigian",
    "carryout",
    "CPI's",
    "Schwarzenegger's",
    "Wu",
    "ciphering",
    "Starkey's",
    "coincided",
    "burnish's",
    "Ngaliema",
    "Lucite's",
    "cordially",
    "England",
    "corporals",
    "backpacker",
    "bail",
    "corset",
    "capability",
    "Amish",
    "Bishop's",
    "bowsprits",
    "Gregg",
    "burgling",
    "concertina's",
    "Atatürk's",
    "buzzword's",
    "Brazilian's",
    "congresswomen",
    "attribute's",
    "Fitch's",
    "butcher",
    "blackball's",
    "corporal's",
    "chewiest",
    "Delbert",
    "Vlasic",
    "bottle",
    "aliases",
    "Romanticism",
    "adjourned",
    "clasps",
    "barrio's",
    "abusers",
    "Australia",
    "bakery",
    "Folgers",
    "bellhop's",
    "awkwardness",
    "converting",
    "biodegradable",
    "bibliophile's",
    "bloomer's",
    "clean",
    "Weissmuller's",
    "barnstorming",
    "comfier",
    "Lulu",
    "bobs",
    "chunk's",
    "codded",
    "cleaver",
    "barracks",
    "assemblywomen",
    "chutzpa",
    "adjusted",
    "bossily",
    "Peg",
    "Luxembourger's",
    "Mead",
    "bracket",
    "candles",
    "Hahn",
    "Shell",
    "Mozambican",
    "Jason",
    "Busch's",
    "Wedgwood",
    "colder",
    "bleakness's",
    "Torres",
    "assembly's",
    "Shari'a",
    "Graffias's",
    "abundances",
    "clubbed",
    "chambray",
    "Styx",
    "Giselle's",
    "annotated",
    "Mohammedans",
    "Cedric's",
    "Veronese",
    "coquettish",
    "birthrate",
    "Pareto",
    "abysses",
    "Thursday",
    "biweekly",
    "buttoned",
    "brawniest",
    "Tuamotu",
    "appraisal",
    "conditional",
    "circumcisions",
    "colony",
    "Gargantua",
    "conning",
    "Sammy's",
    "Caucasians",
    "battlefield's",
    "chenille",
    "birdbath's",
    "Chavez",
    "Beau",
    "Ivory",
    "councilors",
    "clip",
    "balsams",
    "Gaul",
    "Voldemort's",
    "contumacious",
    "Leary",
    "conics",
    "conjoins",
    "contraceptive",
    "Mickie's",
    "Unitarianism",
    "Sindbad",
    "Anglican",
    "Lazarus's",
    "Gleason's",
    "Wednesdays",
    "chauvinist's",
    "Denmark's",
    "Platte's",
    "bullring",
    "Mauro",
    "Mumbai",
    "Gracchus",
    "Rebekah",
    "counterrevolutionary",
    "Beasley",
    "Chattahoochee's",
    "Chad's",
    "Chennai's",
    "Mister",
    "Carole's",
    "cortège",
    "contrary",
    "Landry",
    "Ur",
    "concoctions",
    "acclaimed",
    "Silurian's",
    "clogging",
    "Wheeling",
    "bobby's",
    "Pm",
    "codices",
    "brainwashing",
    "astrology's",
    "Chadwick",
    "Jurua's",
    "Western's",
    "Ikea",
    "aquaplanes",
    "Lethe",
    "absolute",
    "commemorate",
    "Tasmania's",
    "cleverness's",
    "combo",
    "Xerxes",
    "corps",
    "Algonquian",
    "anthropologist's",
    "McDonnell",
    "Chimera's",
    "ancienter",
    "Lobachevsky",
    "Pablo",
    "Bayreuth",
    "Ijssel",
    "children",
    "Urania's",
    "Croats",
    "Panamas",
    "Herrick",
    "Vorster's",
    "Leona",
    "congregate",
    "Constable",
    "Palestinian's",
    "HSBC's",
    "aortae",
    "ambience",
    "bygone's",
    "charter",
    "Calvin's",
    "blearier",
    "Mendoza",
    "contrition",
    "avatars",
    "cairn",
    "conscripts",
    "bacterium's",
    "Renault's",
    "Larson's",
    "bouncers",
    "constancy's",
    "adapting",
    "Cicero's",
    "Akron",
    "bootee",
    "consumer's",
    "Viking's",
    "characterization's",
    "Montserrat",
    "bearer's",
    "Nicodemus",
    "ankle",
    "attentively",
    "Svalbard",
    "competing",
    "caring",
    "Aswan",
    "Zedekiah",
    "arsonists",
    "clarifies",
    "anecdote",
    "Nescafe's",
    "Han",
    "agribusiness's",
    "contrition",
    "cosmetology",
    "abolishes",
    "assize's",
    "Purcell",
    "broadcast",
    "Pound",
    "Norse's",
    "Marquita",
    "Unitarians",
    "Omsk",
    "August",
    "Steele",
    "carom",
    "clarinettist",
    "braided",
    "Zelig",
    "Jacuzzi",
    "Nexis",
    "Donizetti",
    "baptizing",
    "Liebfraumilch",
    "congenially",
    "bested",
    "broiler's",
    "ambiance",
    "anonymity's",
    "Olduvai",
    "clobbers",
    "bulbs",
    "clemency's",
    "chiropody",
    "bronchitis's",
    "Sabrina's",
    "Xavier",
    "Elizabethan",
    "Frenchmen",
    "chi",
    "Waldensian",
    "Topsy's",
    "Niagara",
    "carver's",
    "Wycherley's",
    "brokenhearted",
    "backlash",
    "Rankine",
    "accountant",
    "Ernestine's",
    "arch's",
    "chancellor",
    "Madagascans",
    "Linda",
    "Dane's",
    "Josephine",
    "constrictor's",
    "citing",
    "calendar",
    "aggressiveness",
    "Jephthah",
    "better's",
    "Muslims",
    "Decatur",
    "ambush's",
    "Charolais's",
    "acquaintance's",
    "Boreas",
    "Juanita's",
    "bellboy's",
    "commencing",
    "blancmange",
    "Epsom",
    "Lauri's",
    "affections",
    "avenues",
    "Araceli",
    "Haiti's",
    "Antarctica",
    "afoul",
    "Nunez's",
    "astutely",
    "Rosenzweig's",
    "assassinate",
    "climactic",
    "abusive",
    "char",
    "chauvinists",
    "Rubin's",
    "bailiwick's",
    "Sarasota's",
    "astound",
    "Paige",
    "boardwalk",
    "bourgeoisie's",
    "colludes",
    "carnelian's",
];
