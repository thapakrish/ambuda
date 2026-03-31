"""General information about Ambuda."""

from flask import Blueprint, redirect, render_template, url_for


bp = Blueprint("about", __name__)

people = Blueprint("people", __name__)
bp.register_blueprint(people, url_prefix="/people")


@bp.route("/")
def index():
    return render_template("about/index.html")


@bp.route("/mission")
def mission():
    return render_template("about/mission.html")


@bp.route("/values")
def values():
    return render_template("about/values.html")


@people.route("/", endpoint="index")
def people_index():
    return render_template("about/people.html")


@people.route("/core")
@people.route("/proofing")
def people_redirect():
    return redirect(url_for("about.people.index"))


@bp.route("/code-and-data")
def code_and_data():
    return render_template("about/code-and-data.html")


@bp.route("/our-name")
def name():
    return render_template("about/our-name.html")


@bp.route("/contact")
def contact():
    return render_template("about/contact.html")


@bp.route("/terms")
def terms():
    return render_template("about/terms.html")


@bp.route("/privacy-policy")
def privacy():
    return render_template("about/privacy.html")


@bp.route("/unproofed-texts")
def unproofed_texts():
    return render_template("about/unproofed-texts.html")
