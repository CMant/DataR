#include <glib.h>
#include <ctype.h>
#define APP_NAME "DATAR"
#define CONFIG_FILE "R.ini"

char * Get_Conf_Value(char *kev){
    GKeyFile * config;
    gchar *value;
    config = g_key_file_new();
    g_key_file_load_from_file(config,CONFIG_FILE, 0, NULL);
    value=g_key_file_get_string(config,APP_NAME,kev,NULL);
    g_key_file_free(config);
    return value;
}
char *left_trim(char *str)
{
    char *beginp = str;
    char *tmp = str;
    while(isspace(*beginp)) beginp++;
    while((*tmp++ = *beginp++));
    return str;
}
// delete the back whitespace
char *right_trim(char *str)
{
    char *endp;
    size_t len = strlen(str);
    if(len == 0) return str;
    endp = str + strlen(str) - 1;
    while(isspace(*endp)) endp--;
    *(endp + 1) = '\0';
    return str;
}
char *trim(char *str)
{
    str = left_trim(str);
    str = right_trim(str);
    return str;
}
void FastStrcat(char **Dest, const char* Src,int *len)
 {   
    while(*Src){
        **Dest=*Src;
        *Dest=*Dest+1;
        Src=Src+1;
        (*len)++;
    }
}
