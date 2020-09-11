from flask import Blueprint, render_template

home_bp = Blueprint('hello_bp', __name__)


@home_bp.route('/', methods=['GET'])
# @require_role_admin()
def get_current_user():
    return render_template('index.html')
