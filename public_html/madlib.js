
function validate() {
    var nickname = $("input#nickname").val();
    var drink = $("input:radio[name='drink']:checked").val();
    var course = $("select#courses").val();
    var luckynumber = $("input#lucky_number").val();
    var wishes = $("input:checkbox[name='wishes']:checked");
    
    var num_regex=/^([1-9]\d*)|0$/;
    if (nickname == "") {
        alert("Please enter your nickname."); 
        return false;
    }
    if (drink == undefined) {
        alert("Please choose one drink.");
        return false;
    }
    if (!num_regex.test(luckynumber)) {
        alert("Please enter a positive integer or 0 for your lucky number.");
        return false;
    }
    if (wishes.length == 0) {
        alert("Please check one or more wishes.");
        return false;
    }
    $("#name_value").text(nickname);
    $("#drink_value").text(drink);
    $("#info_value").text(course);
    $("#lucky_value").text(luckynumber);
    var wishlist = $("input:checkbox[name='wishes']:checked").map(function () { return this.value; }).get().join(', ');
    $("#wishes_value").text(wishlist);

    $("#madlib-output").css("display", "block");

    return false;//if true, execute "action" at form tag
}

$(document).ready(function () {
    $("#madlib-input").submit(validate);
    console.log("ready!");
});
