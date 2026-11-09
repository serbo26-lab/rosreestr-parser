# Repository rules for Rosreestr Parser

## Absolute rule: synthetic data only

**Never put real addresses, real cadastral numbers, registration/right numbers, or user-provided property data in this repository.** This applies to every branch and all tests, fixtures, example files, documentation, source comments, commit messages, PR descriptions, and screenshots.

- Use explicitly **fictional** places, streets, house numbers and personal identifiers for all examples.
- For cadastral-number examples use the intentionally impossible fictional form `00:00:000000:1001` (change only the fictional suffix as needed).
- Never paste details from actual program runs, user Excel files, result/log workbooks, screenshots or messages into committed text or tests.
- Real user data may be analyzed outside GitHub, but must be transformed into fictitious equivalents **before** creating any source, fixture, test, issue or PR content.
- Before committing, inspect the complete diff for unintended data and run `python scripts/check_test_fixtures.py`; automated checks are not a substitute for human review.
- If a leak is discovered, stop publication immediately. Remove it from the active branch/history and check PR descriptions and artifacts. A normal revert or file deletion does **not** guarantee removal from GitHub history or caches.

This privacy rule takes priority over convenience when writing tests or debugging.
