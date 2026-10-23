"""Alcohol rule A+B+C. Keyword pass (whole words) first; titles it does not catch
go to a Haiku yes/no check (see llm.alcohol_requests). Ambiguous = alcohol."""
import re

# A: alcoholic drinks
A = ["cocktail", "cocktails", "martini", "martinis", "margarita", "margaritas", "mojito",
     "mojitos", "sangria", "spirits", "wine", "wines", "beer", "beers", "liqueur", "whiskey",
     "whisky", "gin", "vodka", "tequila", "champagne", "prosecco", "mimosa", "mimosas",
     "daiquiri", "daiquiris", "negroni", "bloody mary", "manhattan", "manhattans", "sake",
     "stout", "lager", "boozy", "spiked", "irish cream", "limoncello", "kahlua", "mulled"]
# B: drinks where alcohol is unclear from the title
B = ["punch", "cider", "eggnog", "toddy", "aperitif", "spritz"]
# C: dishes with alcohol in the title
C = ["bourbon", "rum", "ale", "marsala", "sherry", "brandy", "coq au vin", "bourguignon",
     "burgundy", "amaretto", "bellini", "port", "drunken", "tipsy"]

# Siddharth (22 Oct): cocktail-named dishes without alcohol stay in food campaigns.
NOT_ALCOHOL_PHRASES = ["shrimp cocktail", "fruit cocktail", "cocktail sauce", "cocktail cups"]

_KW = sorted(A + B + C, key=len, reverse=True)
_RX = re.compile(r"(?<![a-z])(" + "|".join(re.escape(k) for k in _KW) + r")(?![a-z])", re.I)


def keyword_check(title):
    """Returns (True, reason), (False, reason) for decided food, or (None, None) = ask Haiku."""
    t = title.lower()
    decided = [p for p in NOT_ALCOHOL_PHRASES if p in t]
    for p in decided:
        t = t.replace(p, " ")
    m = _RX.search(t)
    if not m:
        if decided:   # food by Siddharth's decision (22 Oct); never sent to Haiku
            return False, f"not alcohol: '{decided[0]}' dish (Siddharth's decision)"
        return None, None
    word = m.group(1).lower()
    group = "A" if word in A else "B" if word in B else "C"
    return True, f"keyword {group}: {word}"


MUST_TAG = ["Chicken Marsala", "Beer-Batter Fish", "Rum Cake", "Vodka Sauce", "Sherry Trifle",
            "Spiced Apple Cider", "Smoky Maple Bourbon Beef Jerky"]
MUST_NOT_TAG = ["Kale Salad", "Apple Crumble", "Portobello Mushrooms", "Classic Shrimp Cocktail",
                "Avocado and Shrimp Cocktail Cups", "Tropical Breeze Fruit Cocktail Cake"]
