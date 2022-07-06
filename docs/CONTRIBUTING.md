# Contributions

Hi there! We're thrilled that you'd like to contribute to this project. Your help is essential for keeping it great.

Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md).
By participating in this project you agree to abide by its terms.

# Security

If you found a security vulnerability, please, follow the [security guidelines](SECURITY.md).

# Issues, PRs & Discussions

If you have suggestions for how this project could be improved, or want to
report a bug, open an issue! We'd love all and any contributions. If you have questions, too, we'd love to hear them.

We'd also love PRs. If you're thinking of a large PR, we advise opening up an issue first to talk about it,
though! Look at the links below if you're not sure how to open a PR.

If you have other questions, use [Github Discussions](https://github.com/astronomer/astro-sdk/discussions).


# Creating a local development

Follow the steps described in [development](DEVELOPMENT.md).


# Prepare PR

1. Update the local sources to address the issue you are working on.

   * Create a local branch for your development. Make sure to use latest
     [astronomer/astro-sdk/main](https://github.com/astronomer/astro-sdk) as base for the branch. This allows you to easily compare
     changes, have several changes that you work on at the same time and many more.

   * Add necessary code and (unit) tests.

   * Run the unit tests from the IDE or local virtualenv as you see fit.

   * Ensure test coverage is above **90%** for each of the files that you are changing.

   * Run and fix all the static checks. If you have
     pre-commits installed, this step is automatically run while you are committing your code.
     If not, you can do it manually via `git add` and then `pre-commit run`.

2. Remember to keep your branches up to date with the ``main`` branch, squash commits, and
   resolve all conflicts.

3. Re-run static code checks again.

4. Make sure your commit has a good title and description of the context of your change, enough
   for the committer reviewing it to understand why you are proposing a change. Make sure to follow other
   PR guidelines described below.
   Create Pull Request!


# Pull Request Guidelines

Before you submit a pull request (PR), check that it meets these guidelines:

-   Include tests unit tests and example DAGs (wherever applicable) to your pull request.
    It will help you make sure you do not break the build with your PR and that you help increase coverage.

-   [Rebase your fork](http://stackoverflow.com/a/7244456/1110993), and resolve all conflicts.

-   When merging PRs, Committer will use **Squash and Merge** which means then your PR will be merged as one commit,
    regardless of the number of commits in your PR.
    During the review cycle, you can keep a commit history for easier review, but if you need to,
    you can also squash all commits to reduce the maintenance burden during rebase.

-   If your pull request adds functionality, make sure to update the docs as part
    of the same PR. Doc string is often sufficient. Make sure to follow the
    Sphinx compatible standards.

-   Run tests locally before opening PR.

-   Adhere to guidelines for commit messages described in this [article](http://chris.beams.io/posts/git-commit/).
    This makes the lives of those who come after you a lot easier.
