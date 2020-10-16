/* 
This is an index.php that lists all the files in this current directory, makes it easier to click on all the tests:
https://2buntu.com/articles/1301/php-script-to-dynamically-generate-links-to-the-files-present-in-current-directory/
 */
<?php
$dir_open = opendir('.');

while(false !== ($filename = readdir($dir_open))){
    if($filename != "." && $filename != ".."){
        $link = "<a href='./$filename'> $filename </a><br />";
        echo $link;
    }
}

closedir($dir_open);
?>
